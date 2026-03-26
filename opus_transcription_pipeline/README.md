# Opus Audio Transcription Pipeline

A Databricks Spark Declarative Pipeline (SDP) that ingests `.opus` audio files, transcribes them using a Whisper model serving endpoint, and summarizes the transcripts with an LLM.

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌─────────────────┐
│   Bronze     │      │     Silver       │      │      Gold       │
│              │      │                  │      │                 │
│ Auto Loader  │─────▶│ Whisper GPU      │─────▶│ ai_query        │
│ .opus → bin  │      │ endpoint → text  │      │ LLM summary     │
└─────────────┘      └──────────────────┘      └─────────────────┘
```

| Layer | Table | Description |
|-------|-------|-------------|
| **Bronze** | `bronze_audio_files` | Auto Loader incrementally ingests `.opus` files as binary from a Unity Catalog Volume |
| **Silver** | `silver_transcripts` | Calls Whisper model serving endpoint (GPU) with retry + exponential backoff |
| **Gold** | `gold_transcripts` | Materialized view with AI-generated summaries via `ai_query` (Llama 3.3 70B) |

## Prerequisites

- Databricks workspace with Unity Catalog
- A Whisper model serving endpoint (GPU recommended)
- Source `.opus` files in a UC Volume
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) configured

## Configuration

Edit `databricks.yml` to set your environment:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | UC catalog | `users` |
| `schema` | Target schema | `dong_qiaoyang` |
| `source_volume_path` | Volume path with `.opus` files | `/Volumes/users/dong_qiaoyang/man_volume/voice/` |
| `schema_location_base` | Auto Loader schema metadata path | `/Volumes/users/dong_qiaoyang/man_volume/pipeline_metadata/schemas` |
| `whisper_endpoint_url` | Full Whisper endpoint invocation URL | `https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/whisper-large-v3-gpu/invocations` |

## Deploy & Run

```bash
cd opus_transcription_pipeline

# Validate
databricks bundle validate

# Deploy to workspace
databricks bundle deploy

# Run the pipeline
databricks bundle run opus_transcription_etl
```

## Query Results

```sql
SELECT file_name, transcript, summary
FROM users.dong_qiaoyang.gold_transcripts
```
