"""
Bronze layer: Ingest .opus audio files as binary from a Unity Catalog Volume.

Uses Auto Loader (cloudFiles) with binaryFile format to incrementally pick up
new .opus files and store them in a streaming Delta table.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

source_volume_path = spark.conf.get("source_volume_path")
schema_location_base = spark.conf.get("schema_location_base")


@dp.table(
    name="bronze_audio_files",
    comment="Raw .opus audio files ingested as binary from Volume",
    cluster_by=["ingestion_date"],
)
def bronze_audio_files():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", f"{schema_location_base}/bronze_audio_files")
        .option("pathGlobFilter", "*.opus")
        .load(source_volume_path)
        .select(
            F.col("path"),
            F.col("length").alias("file_size_bytes"),
            F.col("modificationTime").alias("file_modified_at"),
            F.col("content").alias("audio_binary"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
            F.current_timestamp().alias("_ingested_at"),
            F.current_date().alias("ingestion_date"),
        )
    )
