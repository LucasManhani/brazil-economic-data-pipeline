from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="bcb_selic_pipeline",
    description="Ingere a Meta Selic e transforma os dados com dbt",
    schedule=None,
    start_date=pendulum.datetime(
        2025,
        1,
        1,
        tz="America/Sao_Paulo",
    ),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["bcb", "selic"],
)
def bcb_selic_pipeline():
    @task.bash
    def ingest_bcb_selic_to_gcs_and_bronze() -> str:
        return (
            "cd /opt/airflow/project && "
            "python src/ingestion/bcb_meta_selic.py"
        )

    @task.bash
    def transform_silver_and_gold_with_dbt() -> str:
        return (
            "cd /opt/airflow/project/brazil_economic_data && "
            "dbt build --project-dir . --profiles-dir . "
            "--no-partial-parse"
        )

    ingestion = ingest_bcb_selic_to_gcs_and_bronze()
    transformation = transform_silver_and_gold_with_dbt()

    ingestion >> transformation


bcb_selic_pipeline()
