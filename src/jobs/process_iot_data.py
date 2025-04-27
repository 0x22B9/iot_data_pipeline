import argparse
import logging
import sys
import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config(config_path="src/config/config.yaml"):
    """Loads configuration from a YAML file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        raise
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        raise


def create_spark_session(config):
    """Creates and returns SparkSession based on config."""
    spark_config = config.get("spark", {})
    app_name = spark_config.get("app_name", "IoTDataProcessing")
    driver_memory = spark_config.get("driver_memory", "4g")
    master = spark_config.get("master", "local[*]")
    adaptive_enabled = spark_config.get("adaptive_enabled", True)

    logging.info(f"Creating SparkSession {app_name} with master {master}")
    spark_builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.adaptive.enabled", adaptive_enabled)
    )

    for key, value in spark_config.items():
        if key not in [
            "app_name",
            "driver_memory",
            "master",
            "adaptive_enabled",
            "driver_class_path",
            "jars",
        ]:
            spark_builder.config(f"spark.{key}", value)

    spark = spark_builder.getOrCreate()
    logging.info(
        "Spark session created successfully.\
                \nSpark Configuration:"
    )
    for key, value in spark.sparkContext.getConf().getAll():
        if "path" in key or "jar" in key:
            logging.info(f"{key} = {value}")

    return spark


IOT_SCHEMA = StructType(
    [
        StructField("uid", StringType(), True),
        StructField("id.orig_h", StringType(), True),
        StructField("id.orig_p", IntegerType(), True),
        StructField("id.resp_h", StringType(), True),
        StructField("id.resp_p", IntegerType(), True),
        StructField("proto", StringType(), True),
        StructField("service", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("orig_bytes", LongType(), True),
        StructField("resp_bytes", LongType(), True),
        StructField("conn_state", StringType(), True),
        StructField("local_orig", StringType(), True),
        StructField("local_resp", StringType(), True),
        StructField("missed_bytes", LongType(), True),
        StructField("history", StringType(), True),
        StructField("orig_pkts", LongType(), True),
        StructField("orig_ip_bytes", LongType(), True),
        StructField("resp_pkts", LongType(), True),
        StructField("resp_ip_bytes", LongType(), True),
        StructField("tunnel_parents", StringType(), True),
        StructField("label", StringType(), True),
        StructField("detailed-label", StringType(), True),
    ]
)


def clean_col_names(df):
    """Cleans column names by replacing dots and hyphens with underscores."""
    logging.info("Cleaning column names...")
    new_columns = [col.replace(".", "_").replace("-", "_") for col in df.columns]
    df_renamed = df.toDF(*new_columns)
    logging.info(f"Renamed columns: {df_renamed.columns}")
    return df_renamed


def transform_data(df):
    """Applies necessary transformations to the DataFrame."""
    logging.info("Starting data transformations...")

    logging.info("Transforming 'duration' column to seconds...")
    df = df.withColumn(
        "duration_parsed",
        F.regexp_extract(
            F.col("duration"), r"(\d+)\s+days\s+(\d{2}):(\d{2}):(\d{2})\.(\d+)", 0
        ),
    )

    df = df.withColumn(
        "duration_sec",
        (
            F.regexp_extract(F.col("duration"), r"(\d+)\s+days", 1).cast(FloatType())
            * 86400.0
        )
        + (
            F.regexp_extract(F.col("duration"), r"days\s+(\d{2}):", 1).cast(FloatType())
            * 3600.0
        )
        + (
            F.regexp_extract(F.col("duration"), r":(\d{2}):", 1).cast(FloatType())
            * 60.0
        )
        + (F.regexp_extract(F.col("duration"), r":(\d{2})\.", 1).cast(FloatType()))
        + (
            F.regexp_extract(F.col("duration"), r"\.(\d+)", 1).cast(FloatType())
            / 1000000.0
        ),
    )
    df = df.withColumn(
        "duration_sec",
        F.when(
            F.col("duration_sec").isNull(),
            F.when(F.col("duration").contains("days"), None).otherwise(
                F.col("duration").cast(FloatType())
            ),
        ).otherwise(F.col("duration_sec")),
    ).drop("duration", "duration_parsed")

    logging.info(
        "Transformation 'duration' finished.\
                \nTransforming 'local_orig' and 'local_resp' to Boolean..."
    )

    df = (
        df.withColumn(
            "local_orig_bool",
            F.when(F.col("local_orig").isNull() | (F.col("local_orig") == ""), False)
            .otherwise(True)
            .cast(BooleanType()),
        )
        .withColumn(
            "local_resp_bool",
            F.when(F.col("local_resp").isNull() | (F.col("local_resp") == ""), False)
            .otherwise(True)
            .cast(BooleanType()),
        )
        .drop("local_orig", "local_resp")
    )
    logging.info(
        "Transformation 'local_orig' and 'local_resp' finished.\
                \nReplacing empty strings with null in 'service' column..."
    )
    df = df.withColumn(
        "service", F.when(F.col("service") == "", None).otherwise(F.col("service"))
    )

    logging.info("Data transformations completed.")
    return df


def write_to_clickhouse(df, config):
    """Writes DataFrame to ClickHouse table."""
    ch_config = config.get("clickhouse")
    if not ch_config:
        logging.warning("ClickHouse configuration not found. Skipping write.")
        return

    url = ch_config.get("jdbc_url")
    table = ch_config.get("table")
    driver = ch_config.get("driver")
    user = ch_config.get("user")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    if not password:
        logging.warning("CLICKHOUSE_PASSWORD environment variable not set. Attempting to read from config (legacy).")
        if not password:
            logging.error("ClickHouse password not found in environment variable CLICKHOUSE_PASSWORD or config.")
            return
    batch_size = ch_config.get("batch_size", 100000)

    if not all([url, table, driver]):
        logging.error(
            "Missing required ClickHouse configuration: url, table, or driver."
        )
        return

    properties = {
        "user": user,
        "password": password,
        "driver": driver,
        "batchsize": str(batch_size),
        "socket_timeout": "300000",
    }

    logging.info(f"Writing data to ClickHouse table: {table} at {url}")
    logging.info(
        f"JDBC Properties: user={user}, driver={driver}, batchsize={batch_size}"
    )

    try:
        df.write.jdbc(url=url, table=table, mode="append", properties=properties)
        logging.info(f"Successfully initiated write to ClickHouse table: {table}")
    except Exception as e:
        logging.error(f"Error writing to ClickHouse: {e}", exc_info=True)


def process_data(spark, config):
    """
    Reads CSV data from input_path (can be a directory), processes it,
    and saves to output_path specified in config.
    """
    input_path = config["paths"]["input_data"]
    output_path = config["paths"]["output_data"]

    logging.info(f"Processing data from {input_path}")
    try:
        df = spark.read.csv(
            input_path,
            header=True,
            schema=IOT_SCHEMA,
            sep=",",
            nullValue="-",
            recursiveFileLookup=True,
            enforceSchema=True,
        )

        logging.info("--- Initial Data ---")
        df.printSchema()

        df_renamed = clean_col_names(df)
        df_transformed = transform_data(df_renamed)

        logging.info("--- Transformed Data ---")
        df_transformed.printSchema()

        if output_path:
            logging.info(f"Saving transformed data to Parquet format at: {output_path}")
            df_transformed.write.mode("overwrite").parquet(output_path)
            logging.info(f"Data saved successfully to Parquet directory: {output_path}")
        else:
            logging.warning(
                "Output path for Parquet is not defined in config. Skipping Parquet write."
            )

    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logging.error(
                f"Error during data processing: Input path does not exist or is empty: {input_path}",
                exc_info=False,
            )
        else:
            logging.error(f"Error during data processing: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="IoT Data Processing with Spark")
    parser.add_argument(
        "--config-path",
        default="src/config/config.yaml",
        help="Path to the configuration YAML file (relative to /app).",
    )
    args = parser.parse_args()

    spark = None
    exit_code = 0
    try:
        config = load_config(args.config_path)
        spark = create_spark_session(config)
        process_data(spark, config)
    except FileNotFoundError:
        logging.error(
            f"Config file not found at {args.config_path}. Check the path and volume mounts."
        )
        exit_code = 1
    except Exception as e:
        logging.error(f"Critical error occurred: {e}.", exc_info=True)
        exit_code = 1
    finally:
        if spark:
            logging.info("Stopping Spark session.")
            spark.stop()
        if exit_code != 0:
            logging.error(f"Exiting with error code {exit_code}")
            sys.exit(exit_code)
        else:
            logging.info("Process finished successfully.")


if __name__ == "__main__":
    main()
