# dags/jaffle_shop.py
from cosmos.providers.dbt.dag import DbtDag
from airflow.datasets import Dataset
from datetime import datetime

jaffle_shop = DbtDag(
		dag_id="jaffle_shop",
    dbt_project_name="jaffle_shop",
    start_date=datetime(2023, 1, 1),
    schedule=[Dataset(f"SEED://JAFFLE_SHOP")],
    conn_id="postgres",
    dbt_args={
        "schema": "public",
        "dbt_executable_path": "/usr/local/airflow/dbt_venv/bin/dbt",
        },
)

jaffle_shop