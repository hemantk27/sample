from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

UTC = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email": ["finance-support@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_etl_dag",
    description="Migrated UC4 workflow for FINANCE_ETL processing.",
    default_args=default_args,
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "etl", "uc4_migration"],
    orientation="LR",
) as dag:

    load_dim = ExternalTaskSensor(
        task_id="load_dim",
        external_dag_id="load_dim_dag",
        external_task_id="load_dim",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    finance_etl = BashOperator(
        task_id="finance_etl",
        bash_command="sh run_etl.sh",
        execution_timeout=timedelta(hours=1),
        env={
            "AIRFLOW_CTX_DAG_ID": "{{ dag.dag_id }}",
            "AIRFLOW_CTX_TASK_ID": "{{ task.task_id }}",
            "AIRFLOW_CTX_EXECUTION_DATE": "{{ ds }}",
            "ETL_ENV": "prod",
        },
    )

    load_dim >> finance_etl

dag_id = dag.dag_id
