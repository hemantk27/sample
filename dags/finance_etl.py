from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

LOCAL_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=120),
}

with DAG(
    dag_id="finance_etl",
    description="Migrated UC4 workflow for FINANCE_ETL from FINANCE_APP",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["uc4-migration", "finance", "etl"],
    default_view="graph",
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=300,
        timeout=14400,
        mode="reschedule",
    )

    finance_etl_task = BashOperator(
        task_id="finance_etl_task",
        bash_command="sh run_etl.sh",
        append_env=True,
        do_xcom_push=False,
        env={
            "TZ": "UTC",
        },
    )

    wait_for_load_dim >> finance_etl_task

dag_id = dag.dag_id