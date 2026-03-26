"""
Silver layer: Transcribe .opus audio files using the Whisper model serving endpoint.

Reads binary audio from the bronze table, sends each file to the Whisper
endpoint via REST API, and stores the resulting transcripts.

Includes retry logic with exponential backoff for transient errors (502/503/504).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Full endpoint URL from pipeline configuration
_endpoint_url = spark.conf.get("whisper_endpoint_url")

# Get token via dbutils (works on serverless)
_api_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Retry settings
_max_retries = 5
_initial_backoff = 10  # seconds


@F.udf(StringType())
def transcribe_audio(audio_bytes: bytes) -> str:
    """Call the Whisper model serving endpoint with retry logic.

    Tries the standard MLflow pyfunc binary input format:
    dataframe_split with base64-encoded bytes column.
    """
    import base64
    import requests
    import json
    import time

    if audio_bytes is None:
        return None

    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")

    # Whisper pyfunc expects: audio (base64), language, task, return_timestamps
    payload = {
        "dataframe_split": {
            "columns": ["audio", "language", "task", "return_timestamps"],
            "data": [[audio_b64, "en", "transcribe", "false"]],
        }
    }

    headers = {
        "Authorization": f"Bearer {_api_token}",
        "Content-Type": "application/json",
    }

    last_error = None
    for attempt in range(_max_retries):
        try:
            resp = requests.post(
                _endpoint_url,
                headers=headers,
                json=payload,
                timeout=300,
            )
        except Exception as e:
            last_error = f"request failed - {str(e)}"
            time.sleep(_initial_backoff * (2 ** attempt))
            continue

        if resp.status_code == 200:
            result = resp.json()
            if "predictions" in result:
                pred = result["predictions"]
                if isinstance(pred, list) and len(pred) > 0:
                    item = pred[0]
                    if isinstance(item, dict) and "text" in item:
                        return item["text"]
                    if isinstance(item, str):
                        return item
                    return str(item)
                return str(pred)
            return json.dumps(result)

        # Retry on transient gateway errors
        if resp.status_code in (429, 502, 503, 504):
            last_error = f"{resp.status_code} - {resp.text[:200]}"
            time.sleep(_initial_backoff * (2 ** attempt))
            continue

        # Non-retryable error — return full detail for debugging
        return f"ERROR: {resp.status_code} - {resp.text[:500]}"

    return f"ERROR: exhausted {_max_retries} retries, last: {last_error}"


@dp.table(
    name="silver_transcripts",
    comment="Transcribed audio files with text output from Whisper",
    cluster_by=["ingestion_date"],
)
def silver_transcripts():
    return (
        spark.read.table("bronze_audio_files")
        .withColumn("transcript", transcribe_audio(F.col("audio_binary")))
        .select(
            F.col("file_name"),
            F.col("path").alias("source_path"),
            F.col("file_size_bytes"),
            F.col("file_modified_at"),
            F.col("transcript"),
            F.when(
                F.col("transcript").startswith("ERROR:"), F.lit("FAILED")
            )
            .otherwise(F.lit("SUCCESS"))
            .alias("transcription_status"),
            F.col("_ingested_at"),
            F.col("ingestion_date"),
            F.current_timestamp().alias("_transcribed_at"),
        )
    )
