# -*- coding: utf-8 -*-
"""Client for the csv-dump-resource endpoint."""

from __future__ import annotations

import io
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry

    _RETRY_KW = dict(allowed_methods=frozenset(["GET"]))
except Exception:
    from urllib3.util.retry import Retry  # type: ignore

    _RETRY_KW = dict(method_whitelist=frozenset(["GET"]))  # type: ignore

from notebookutils import mssparkutils  # noqa: F401  # Imported to ensure Fabric utilities are loaded
from pyspark.sql import DataFrame, SparkSession, functions as F

# -----------------------
# Configuration
# -----------------------
BASE_URL = "https://api.fiskeridir.no/pub-aqua"
TIMEOUT = (10, 120)
USER_AGENT = "pub-aqua-csv-dump-client/1.0"
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5
STATUS_FORCELIST = (429, 500, 502, 503, 504)

LAKEHOUSE_DB = "LH_ClientDemo"
BRONZE_SCHEMA = "A_Bronze"
CSV_DUMP_TABLE_NAME = "PubAqua_CsvDump_Raw"
CSV_DUMP_BRONZE_TABLE = f"`{BRONZE_SCHEMA}`.`{CSV_DUMP_TABLE_NAME}`"

CSV_DUMP_ENDPOINT = "/api/v1/dump/new-legacy-csv"
DEFAULT_PREVIEW_LIMIT = 25


# -----------------------
# HTTP session with retry
# -----------------------
def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=STATUS_FORCELIST,
        raise_on_status=False,
        respect_retry_after_header=True,
        **_RETRY_KW,
    )
    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30),
    )
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/csv, text/plain;q=0.9, */*;q=0.1",
        }
    )
    return session


SESSION = _build_session()


# -----------------------
# Helpers
# -----------------------
def _ensure_bronze_schema() -> None:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{BRONZE_SCHEMA}`")


def _parse_csv_to_spark(csv_text: str) -> Optional[DataFrame]:
    if not csv_text.strip():
        return None

    pdf = pd.read_csv(io.StringIO(csv_text), sep=None, engine="python")
    if pdf.empty:
        return None

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark.createDataFrame(pdf)


# -----------------------
# API
# -----------------------
def fetch_csv_dump() -> str:
    url = f"{BASE_URL}{CSV_DUMP_ENDPOINT}"
    response = SESSION.get(url, timeout=TIMEOUT)
    if response.status_code >= 400:
        raise requests.HTTPError(
            f"{response.status_code} {response.reason} for {response.url} :: {response.text}"
        )
    return response.text


# -----------------------
# Lakehouse load + preview
# -----------------------
def load_csv_dump_to_bronze(overwrite: bool = True) -> int:
    csv_text = fetch_csv_dump()
    df = _parse_csv_to_spark(csv_text)
    if df is None:
        print("No data received from csv-dump-resource endpoint")
        return 0

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    _ensure_bronze_schema()

    df = df.withColumn("ingestion_ts", F.current_timestamp())

    writer = (
        df.write.format("delta").option("overwriteSchema", "true")
    )
    writer = writer.mode("overwrite" if overwrite else "append")
    writer.saveAsTable(CSV_DUMP_BRONZE_TABLE)

    return df.count()


def preview_csv_dump(limit: int = DEFAULT_PREVIEW_LIMIT):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    preview_df = (
        spark.table(CSV_DUMP_BRONZE_TABLE)
        .orderBy(F.col("ingestion_ts").desc())
        .limit(limit)
    )

    try:
        display(preview_df)
    except Exception:
        from IPython.display import display as ipy_display

        ipy_display(preview_df.toPandas())


if __name__ == "__main__":
    total_rows = load_csv_dump_to_bronze(overwrite=True)
    print(
        f"Previewing the first rows from {CSV_DUMP_BRONZE_TABLE} (total written: {total_rows})"
    )
    preview_csv_dump(limit=DEFAULT_PREVIEW_LIMIT)
