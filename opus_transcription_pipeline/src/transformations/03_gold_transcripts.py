"""
Gold layer: Summarize transcripts using ai_query.

Calls a foundation model to generate a concise summary of each transcript.
Only includes successfully transcribed files.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_transcripts",
    comment="Transcripts with AI-generated summaries",
)
def gold_transcripts():
    return (
        spark.read.table("silver_transcripts")
        .filter(F.col("transcription_status") == "SUCCESS")
        .withColumn(
            "summary",
            F.expr(
                """ai_query(
                    'databricks-meta-llama-3-3-70b-instruct',
                    CONCAT(
                        'Summarize the following audio transcript in 2-3 concise sentences. '
                        'Focus on the key topics and action items. Transcript: ',
                        transcript
                    )
                )"""
            ),
        )
        .select(
            F.col("file_name"),
            F.col("source_path"),
            F.col("transcript"),
            F.col("summary"),
            F.col("file_size_bytes"),
            F.col("file_modified_at"),
            F.col("_ingested_at"),
            F.col("_transcribed_at"),
        )
    )
