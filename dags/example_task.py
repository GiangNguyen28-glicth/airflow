from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "giangnt",
}

with DAG(
    dag_id="spark_submit_example",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False
):
    spark_job = SparkSubmitOperator(
        task_id="spark_pi",
        application="/opt/spark-apps/pi.py",     # path to your spark app
        conn_id="spark_default",                # Airflow Spark Connection
        verbose=True,
        executor_memory="1g",
        driver_memory="512m"
    )

    spark_job
