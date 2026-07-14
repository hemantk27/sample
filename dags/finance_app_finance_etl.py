from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_app_finance_etl",
    description="Migrated UC4 workflow for FINANCE_ETL from FINANCE_APP",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["finance", "uc4_migration", "etl"],
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=3600,
    )

    run_finance_etl = BashOperator(
        task_id="run_finance_etl",
        bash_command=(
            "sh {{ var.value.finance_etl_script_path | "
            "default('/opt/airflow/scripts/finance/run_etl.sh') }}"
        ),
        execution_timeout=timedelta(hours=1),
        append_env=True,
        env={
            "TZ": Variable.get(
                "finance_runtime_timezone",
                default_var="UTC",
            )
        },
    )

    wait_for_load_dim >> run_finance_etl
