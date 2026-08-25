import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados"

def extract_selic(start_date: str, end_date: str) -> list[dict[str, str]]:
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

    return response.json()


if __name__ == "__main__":
    records = extract_selic(
        start_date="01/01/2024",
        end_date="31/01/2024",
    )

    print(f"Quantidade de registros: {len(records)}")

    for record in records[:5]:
        print(record)