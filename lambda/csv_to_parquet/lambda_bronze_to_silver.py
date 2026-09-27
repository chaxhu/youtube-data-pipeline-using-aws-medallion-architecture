"""
Lambda: Video Statistics CSV → Silver Layer (Parquet)
──────────────────────────────────────────────────────
Triggered by S3 event when new CSV lands in the Bronze bucket
(yt-data-pipeline-bronze-ap-london-1-dev) under:
  youtube/raw_statistics/region=<region>/*.csv

Features:
  - Data validation before writing
  - Deduplication of video records (same video_id + trending_date)
  - Proper error handling with dead-letter alerting (SNS, optional)
  - Idempotent writes (overwrites partition, not append)
  - Structured logging
  - Registers/updates the table in the Glue Data Catalog

Environment Variables:
    S3_BUCKET_SILVER            — Target bucket for cleansed data
                                   (default: yt-data-pipeline-silver-ap-london-1-dev)
    GLUE_DB_SILVER              — Glue catalog database name (default: yt_pipeline_silver_dev)
    GLUE_TABLE_STATISTICS       — Glue catalog table name (default: clean_video_statistics)
    SNS_ALERT_TOPIC_ARN         — SNS topic for alerts (optional)

Requires the "AWSSDKPandas-Python3xx" Lambda layer (awswrangler + pandas + pyarrow).
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────
SILVER_BUCKET = os.environ.get("S3_BUCKET_SILVER", "yt-data-pipeline-silver-ap-london-1-dev")
GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_STATISTICS", "clean_video_statistics")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/raw_statistics/"

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a *_videos.csv file (e.g. CAvideos.csv) into a DataFrame.

    ISO-8859-1 is used because the Kaggle YouTube trending export contains
    non-UTF-8 characters in video titles/descriptions across regions.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"], encoding="ISO-8859-1", on_bad_lines="skip")


def validate_video_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean the video statistics data.
    Returns cleaned DataFrame or raises ValueError.
    """
    if df.empty:
        raise ValueError("Empty DataFrame — no video rows found")

    required_cols = {"video_id", "trending_date", "views", "likes", "dislikes"}
    actual_cols = set(df.columns)
    missing = required_cols - actual_cols
    if missing:
        logger.warning(f"Missing expected columns: {missing}. Available: {actual_cols}")

    # Drop duplicate rows (same video on the same trending date)
    before = len(df)
    dedup_cols = [c for c in ("video_id", "trending_date") if c in df.columns]
    if dedup_cols:
        df = df.drop_duplicates(subset=dedup_cols, keep="last")
    after = len(df)
    if before != after:
        logger.info(f"  Removed {before - after} duplicate video rows")

    return df


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    """Process S3 event for new video statistics CSV files."""

    records = event.get("Records", [])
    if not records:
        records = [event] if "s3" in event else []

    processed = []
    errors = []

    for record in records:
        key = None
        try:
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            key = unquote_plus(s3_info["object"]["key"])

            logger.info(f"Processing: s3://{bucket}/{key}")

            # ── Read raw CSV ─────────────────────────────────────────────
            df = read_csv_from_s3(bucket, key)
            logger.info(f"  Raw shape: {df.shape}")

            # ── Validate ─────────────────────────────────────────────────
            df = validate_video_data(df)

            # ── Add metadata columns ─────────────────────────────────────
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            df["_source_file"] = key

            # Extract region from the S3 key (e.g., region=US)
            region = "unknown"
            for part in key.split("/"):
                if part.startswith("region="):
                    region = part.split("=")[1]
                    break
            df["region"] = region

            logger.info(f"  Clean shape: {df.shape}, region: {region}")

            # ── Write to Silver layer as Parquet ─────────────────────────
            wr.s3.to_parquet(
                df=df,
                path=SILVER_PATH,
                dataset=True,
                database=GLUE_DB,
                table=GLUE_TABLE,
                partition_cols=["region"],
                mode="overwrite_partitions",  # Idempotent per region
                schema_evolution=True,
            )

            logger.info(f"  Written to Silver: {SILVER_PATH}")
            processed.append({"key": key, "region": region, "rows": len(df)})

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            errors.append({"key": key or "unknown", "error": str(e)})

    if errors:
        send_alert(
            subject="[YT Pipeline] Silver video statistics transform failed",
            message=json.dumps(errors, indent=2),
        )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors,
    }
