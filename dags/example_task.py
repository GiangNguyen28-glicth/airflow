from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "giangnt",
}

with DAG(
    dag_id="spark_submit_example",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    default_args=default_args,
    catchup=False
):
    spark_job = SparkSubmitOperator(
        task_id="spark_pi",
        application="dags/word_count.py",     # path to your spark app
        conn_id="spark_default",                # Airflow Spark Connection
        verbose=True,
        conf={
            'spark.master': 'spark://10.36.241.102:7077',
            'spark.submit.deployMode': 'client',
        },
        executor_memory="1g",
        driver_memory="512m"
    )

    spark_job
