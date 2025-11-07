from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hàm thực thi Python
def start_task():
    print("✅ Job started!")

def process_data():
    print("⚙️ Processing data...")
    # Giả lập xử lý
    for i in range(3):
        print(f"Step {i+1}/3 done")
    print("🎉 Job finished successfully!")

# Định nghĩa DAG
with DAG(
    dag_id="example_job",  # Tên DAG
    description="Simple example job for Airflow",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["example", "demo"],
) as dag:

    task_start = PythonOperator(
        task_id="start_task",
        python_callable=start_task,
    )

    task_process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Định nghĩa thứ tự chạy
    task_start >> task_process
