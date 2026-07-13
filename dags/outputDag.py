from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="finance_etl_dag",
    start_date=datetime(2026, 7, 5),
    schedule_interval="0 2 * * *",
    catchup=False,
    description="Finance ETL pipeline DAG",
    tags=["finance", "etl"],
) as dag:

    load_dim = BashOperator(
        task_id="load_dim",
        bash_command="sh load_dim.sh"
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh run_etl.sh"
    )

    load_dim >> finance_etl