import os
from datetime import date, datetime, timezone

import requests
from dotenv import load_dotenv
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage


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


if __name__ == "__main__":
    load_dotenv()

    project_id = os.environ["GCP_PROJECT_ID"]
    bucket_name = os.environ["GCS_RAW_BUCKET"]

    today = date.today()
    period_start = date(today.year, 1, 1)

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

    print(f"Quantidade de registros: {len(records)}")

    if uploaded:
        print(f"Arquivo enviado: gs://{bucket_name}/{object_name}")
    else:
        print("O arquivo já existe e não foi sobrescrito.")
