import os
from datetime import date, datetime, timezone

import requests
from dotenv import load_dotenv
from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage


SERIES_ID = 432
BASE_URL = (
    f"https://api.bcb.gov.br/dados/serie/"
    f"bcdata.sgs.{SERIES_ID}/dados"
)


def extract_selic(
    start_date: str,
    end_date: str,
) -> requests.Response:
    params = {
        "formato": "json",
        "dataInicial": start_date,
        "dataFinal": end_date,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response


def upload_raw_to_gcs(
    raw_content: bytes,
    project_id: str,
    bucket_name: str,
    object_name: str,
) -> bool:
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    try:
        blob.upload_from_string(
            raw_content,
            content_type="application/json",
            if_generation_match=0,
        )
    except PreconditionFailed:
        return False

    return True


def load_to_bigquery(
    records: list[dict[str, str]],
    project_id: str,
    dataset_id: str,
) -> tuple[int, int]:
    client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset_id}.meta_selic"
    staging_table_id = f"{project_id}.{dataset_id}.meta_selic_staging"

    schema = [
        bigquery.SchemaField("data", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("valor", "STRING", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        records,
        staging_table_id,
        job_config=job_config,
    )
    load_job.result()

    merge_query = f"""
        MERGE `{table_id}` AS target
        USING `{staging_table_id}` AS source
        ON target.data = source.data
        WHEN MATCHED
            AND target.valor IS DISTINCT FROM source.valor THEN
            UPDATE SET valor = source.valor
        WHEN NOT MATCHED THEN
            INSERT (data, valor)
            VALUES (source.data, source.valor)
    """

    merge_job = client.query(merge_query)
    merge_job.result()

    client.delete_table(staging_table_id, not_found_ok=True)

    staged_rows = load_job.output_rows or 0
    affected_rows = merge_job.num_dml_affected_rows or 0

    return staged_rows, affected_rows


if __name__ == "__main__":
    load_dotenv()

    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_RAW_BUCKET"]
    dataset_id = os.environ["BQ_DATASET"]

    today = date.today()
    period_start = date(2025, 1, 1)

    start_date = period_start.strftime("%d/%m/%Y")
    end_date = today.strftime("%d/%m/%Y")

    response = extract_selic(
        start_date=start_date,
        end_date=end_date,
    )

    extraction_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    object_name = (
        f"bcb/sgs/series_id={SERIES_ID}/"
        f"extraction_date={extraction_date}/"
        f"period={period_start.isoformat()}_{today.isoformat()}/"
        "data.json"
    )

    uploaded = upload_raw_to_gcs(
        raw_content=response.content,
        project_id=project_id,
        bucket_name=bucket_name,
        object_name=object_name,
    )

    records = response.json()

    staged_rows, affected_rows = load_to_bigquery(
        records=records,
        project_id=project_id,
        dataset_id=dataset_id,
    )

    print(f"Quantidade de registros: {len(records)}")
    print(f"Registros preparados no staging: {staged_rows}")
    print(
        "Registros inseridos ou atualizados no BigQuery: "
        f"{affected_rows}"
    )

    if uploaded:
        print(f"Arquivo enviado: gs://{bucket_name}/{object_name}")
    else:
        print("O arquivo já existe e não foi sobrescrito.")
