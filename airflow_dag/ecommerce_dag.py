from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "ecommerce_pipeline",
    start_date=datetime(2025, 9, 29),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    start_kafka = BashOperator(
        task_id="start_kafka",
        bash_command="echo 'Starting Kafka Producer...'"
    )

    run_spark = BashOperator(
        task_id="run_spark",
        bash_command="spark-submit /opt/airflow/dags/spark_stream.py"
    )

    start_kafka >> run_spark
