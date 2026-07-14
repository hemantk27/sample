from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

local_tz = pendulum.timezone("UTC")

with DAG(
    dag_id="finance_etl_dag",
    description="Migrated UC4 workflow for FINANCE_ETL job",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finance", "uc4-migration", "etl", "daily"],
) as dag:

    wait_for_load_dim = ExternalTaskSensor(
        task_id="wait_for_load_dim",
        external_dag_id="load_dim_dag",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
    )

    run_finance_etl = BashOperator(
        task_id="run_finance_etl",
        bash_command="sh {{ var.value.finance_etl_script_path }}/run_etl.sh",
        cwd="{{ var.value.finance_etl_script_path }}",
        append_env=True,
        env={
            "APP_NAME": "FINANCE_APP",
            "JOB_NAME": "FINANCE_ETL",
        },
        execution_timeout=timedelta(hours=2),
    )

    validate_etl_completion = BashOperator(
        task_id="validate_etl_completion",
        bash_command="echo 'ETL completed successfully for FINANCE_ETL'",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    wait_for_load_dim >> run_finance_etl >> validate_etl_completion