from __future__ import annotations

import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    ch_user = Variable.get("iot_ch_user")
    ch_password = Variable.get("iot_ch_password")
    ch_db = Variable.get("iot_ch_db")
    ch_table = Variable.get("iot_ch_table")
    ch_container_name = Variable.get("iot_ch_container_name")
    spark_container_name = Variable.get("iot_spark_container_name")
    spark_parquet_dir_in_spark = Variable.get("iot_spark_parquet_dir_in_spark")
    ch_parquet_dir_in_ch = Variable.get("iot_ch_parquet_dir_in_ch")
except KeyError as e:
    raise ValueError(f"Airflow Variable not set: {e}")

SPARK_JAR_PATH_IN_CONTAINER = "/app/jars/clickhouse-jdbc-0.8.4-all.jar"
SPARK_SCRIPT_PATH_IN_CONTAINER = "/app/src/jobs/process_iot_data.py"
SPARK_CONFIG_PATH_IN_CONTAINER = "/app/src/config/config.yaml"

with DAG(
    dag_id="iot_data_processing_pipeline_parquet",
    start_date=pendulum.datetime(2025, 4, 27, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["iot", "spark", "docker", "clickhouse", "parquet", "bash"],
) as dag:
    start = EmptyOperator(task_id="start")

    cleanup_previous_parquet = BashOperator(
        task_id="cleanup_previous_parquet",
        bash_command=f"docker exec {spark_container_name} rm -rf {spark_parquet_dir_in_spark}",
    )

    spark_submit_command = f"""
    docker exec \\
        -e CLICKHOUSE_PASSWORD='{ch_password}' \\
        {spark_container_name} spark-submit \\
        --master local[*] \\
        --jars {SPARK_JAR_PATH_IN_CONTAINER} \\
        {SPARK_SCRIPT_PATH_IN_CONTAINER} \\
        --config-path {SPARK_CONFIG_PATH_IN_CONTAINER}
    """
    run_spark_job = BashOperator(
        task_id="run_spark_to_parquet_job",
        bash_command=spark_submit_command,
        append_env=True,
    )

    truncate_clickhouse_table = BashOperator(
        task_id="truncate_clickhouse_table",
        bash_command=f"""
        docker exec {ch_container_name} clickhouse-client \\
            --user '{ch_user}' \\
            --password '{ch_password}' \\
            --query "TRUNCATE TABLE IF EXISTS {ch_db}.{ch_table}"
        """,
    )

    load_parquet_command = f"""
    docker exec {ch_container_name} clickhouse-client \\
        --user '{ch_user}' \\
        --password '{ch_password}' \\
        --query "INSERT INTO {ch_db}.{ch_table} SELECT * FROM file('{ch_parquet_dir_in_ch}/*.parquet', 'Parquet')"
    """

    load_parquet_to_clickhouse = BashOperator(
        task_id="load_parquet_to_clickhouse",
        bash_command=load_parquet_command,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    (
        start
        >> cleanup_previous_parquet
        >> run_spark_job
        >> truncate_clickhouse_table
        >> load_parquet_to_clickhouse
        >> end
    )
