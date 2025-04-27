from __future__ import annotations

import os

import pendulum

try:
    from docker.types import Mount
except ImportError:
    print("ERROR: Docker SDK not installed. Please install 'docker' library.")
    Mount = None

from airflow.exceptions import AirflowConfigException
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule

COMPOSE_PROJECT_NAME = "iot_data_pipeline"
DOCKER_NETWORK = f"{COMPOSE_PROJECT_NAME}_iot_network"
SPARK_JAR_PATH_IN_CONTAINER = "/app/jars/clickhouse-jdbc-0.8.4-all.jar"
SPARK_SCRIPT_PATH_IN_CONTAINER = "/app/src/jobs/process_iot_data.py"
SPARK_CONFIG_PATH_IN_CONTAINER = "/app/src/config/config.yaml"
SPARK_CONTAINER_USER = "1001"

HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH")
if not HOST_PROJECT_PATH:
    raise AirflowConfigException(
        "Environment variable HOST_PROJECT_PATH is not set in .env.airflow. "
        "Set it to the absolute path of the project root on the host machine (use '/' separators)."
    )

print(f"DEBUG: Using HOST_PROJECT_PATH='{HOST_PROJECT_PATH}'")

VOLUMES_LIST = [
    f"{HOST_PROJECT_PATH}/src:/app/src:ro",
    f"{HOST_PROJECT_PATH}/data:/app/data",
    f"{HOST_PROJECT_PATH}/jars:/app/jars:ro",
]
print(f"DEBUG: Constructed volumes for DockerOperator: {VOLUMES_LIST}")

if Mount is None:
    raise AirflowConfigException("Docker SDK 'Mount' type could not be imported.")

MOUNTS_LIST = [
    Mount(
        target="/app/src",
        source=f"{HOST_PROJECT_PATH}/src",
        type="bind",
        read_only=True,
    ),
    Mount(
        target="/app/data",
        source=f"{HOST_PROJECT_PATH}/data",
        type="bind",
        read_only=False,
    ),
    Mount(
        target="/app/jars",
        source=f"{HOST_PROJECT_PATH}/jars",
        type="bind",
        read_only=True,
    ),
]
print(f"DEBUG: Constructed mounts for DockerOperator: {MOUNTS_LIST}")

with DAG(
    dag_id="iot_data_processing_pipeline",
    start_date=pendulum.datetime(2025, 4, 27, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["iot", "spark", "docker", "clickhouse"],
) as dag:
    start = DockerOperator(
        task_id="start_placeholder",
        image="bash:latest",
        command="echo 'Starting IoT Data Processing...'",
        auto_remove=True,
    )

    run_spark_job = DockerOperator(
        task_id="run_spark_iot_job",
        image="iot-spark-app:latest",
        command=[
            "spark-submit",
            "--master",
            "local[*]",
            "--jars",
            SPARK_JAR_PATH_IN_CONTAINER,
            SPARK_SCRIPT_PATH_IN_CONTAINER,
            "--config-path",
            SPARK_CONFIG_PATH_IN_CONTAINER,
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        user=SPARK_CONTAINER_USER,
        mounts=MOUNTS_LIST,
        auto_remove=True,
        mount_tmp_dir=False,
        tty=False,
        environment={
            "CLICKHOUSE_HOST": "clickhouse-server",
        },
    )

    end = DockerOperator(
        task_id="end_placeholder",
        image="bash:latest",
        command="echo 'IoT Data Processing finished.'",
        auto_remove=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> run_spark_job >> end
