from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    DAG = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# Add both project root and airflow_home/dags to path so 'src' imports work
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_INPUT = str(PROJECT_ROOT / "data" / "orders_passed.csv")


def validate_orders_task(input_file: str | None = None) -> dict:
    from src.validation import run_lab_check
    if input_file is None:
        input_file = DEFAULT_INPUT
    summary = run_lab_check(
        input_path=input_file,
        allow_failure=False,
        skip_discord=False,
    )
    return summary


if DAG is not None:
    with DAG(
        dag_id="sales_data_quality_pipeline",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["lab", "data-quality", "discord"],
    ) as dag:
        validate_orders = PythonOperator(
            task_id="validate_orders",
            python_callable=validate_orders_task,
            op_args=["{{ dag_run.conf.get('input_file', '" + DEFAULT_INPUT + "') if dag_run and dag_run.conf is not none else '" + DEFAULT_INPUT + "' }}"],
        )
else:  # pragma: no cover
    dag = None
