# File: dags/test_basic_dag.py

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag(
    dag_id="test_basic_dag",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=5),   # Airflow 3 accepts `schedule` (alias of schedule_interval)
    catchup=False,
    max_active_runs=1,
    tags=["test", "smoke"],
)
def test_basic_dag():
    @task
    def say_hello() -> str:
        return "hello-airflow-3"

    hello = say_hello()

    bash = BashOperator(
        task_id="echo_context",
        bash_command="echo DAG_RUN={{ ds }} VALUE={{ ti.xcom_pull(task_ids='say_hello') }}",
    )

    hello >> bash

dag = test_basic_dag()
