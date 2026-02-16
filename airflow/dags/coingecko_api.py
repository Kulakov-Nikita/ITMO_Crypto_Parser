from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import os

import pandas as pd
import requests

from airflow import DAG
from airflow.decorators import task

# Папки для данных (локально)
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

API_TOKEN = os.environ["API_TOKEN"]


def _api_get(coin: str = "bitcoin", vs_currency: str = "usd") -> pd.DataFrame:

    url_market_chart = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart?vs_currency={vs_currency}&days=1"

    headers = {"x-cg-demo-api-key": API_TOKEN}

    market_chart = json.loads(requests.get(url_market_chart, headers=headers).text)

    prices = pd.DataFrame(market_chart.get("prices", []), columns=["ts_ms", "prices"])
    mcaps = pd.DataFrame(market_chart.get("market_caps", []), columns=["ts_ms", "market_caps"])
    vols = pd.DataFrame(market_chart.get("total_volumes", []), columns=["ts_ms", "total_volumes"])

    return prices.merge(mcaps, on="ts_ms", how="outer").merge(vols, on="ts_ms", how="outer")


with DAG(
    dag_id="coingecko_etl",
    start_date=datetime.today(),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["local", "api", "dataset"],
) as dag:

    @task
    def extract() -> str:
        coin: str = "bitcoin"
        vs_currency: str = "usd"

        data = _api_get(coin, vs_currency)

        out_path = RAW_DIR / f"posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        data.to_csv(out_path, index=False)

        return str(out_path)

    @task
    def transform(raw_path: str) -> str:

        df = pd.read_csv(raw_path)
        
        out_path = PROCESSED_DIR / (Path(raw_path).stem.replace("posts_", "dataset_") + ".parquet")
        df.to_parquet(out_path, index=False)
        return str(out_path)

    raw_path = extract()
    dataset_path = transform(raw_path)
