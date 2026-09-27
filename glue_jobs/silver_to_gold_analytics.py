import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Glue Job: Silver → Gold (Analytics Aggregations)
─────────────────────────────────────────────────
Reads cleansed statistics and reference data from Silver,
joins them, and produces business-level aggregations in the Gold layer.

Gold layer tables are optimized for analytics queries in Athena/QuickSight.

Gold tables produced:
  1. trending_analytics   — Daily trending summaries per region
  2. channel_analytics    — Channel performance metrics
  3. category_analytics   — Category-level trends over time

Job Parameters:
    --JOB_NAME              — Glue job name
    --silver_database       — Silver Glue catalog database
    --gold_bucket           — Gold S3 bucket
    --gold_database         — Gold Glue catalog database
"""

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]


# ── Read Silver Tables ──────────────────────────────────────────────────────
logger.info("Reading Silver layer tables...")

stats_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB,
    table_name="clean_statistics",
    transformation_ctx="stats",
)
stats_df = stats_dyf.toDF()
logger.info(f"Statistics records: {stats_df.count()}")

# ── Read Reference Data (optional) + Build category lookup ───────────────────
logger.info("Attempting to read Silver reference data for category names...")

try:
    ref_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB,
        table_name="clean_reference_data",
        transformation_ctx="ref",
    )
    ref_df = ref_dyf.toDF()

    category_lookup = None

    # Some crawlers flatten nested fields in different ways; handle common cases
    if "id" in ref_df.columns and "snippet.title" in ref_df.columns:
        category_lookup = ref_df.select(
            F.col("id").cast("long").alias("category_id"),
            F.col("`snippet.title`").alias("category_name"),
        ).dropDuplicates(["category_id"])

    elif "id" in ref_df.columns and "snippet_title" in ref_df.columns:
        category_lookup = ref_df.select(
            F.col("id").cast("long").alias("category_id"),
            F.col("snippet_title").alias("category_name"),
        ).dropDuplicates(["category_id"])

    else:
        logger.warn(
            "Could not find expected category title columns in reference data. "
            f"Columns found: {ref_df.columns}"
        )

    if category_lookup is not None:
        logger.info(f"Category lookup entries: {category_lookup.count()}")

        # Ensure join key types match
        if "category_id" in stats_df.columns:
            stats_df = stats_df.withColumn("category_id", F.col("category_id").cast("long"))

        stats_df = stats_df.join(
            F.broadcast(category_lookup),
            on="category_id",
            how="left",
        )

except Exception as e:
    logger.warn(f"Could not load reference data: {e}. Proceeding without category names.")

# ✅ Always guarantee category_name exists
if "category_name" not in stats_df.columns:
    stats_df = stats_df.withColumn("category_name", F.lit("Unknown"))
else:
    stats_df = stats_df.fillna("Unknown", subset=["category_name"])

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 1: Trending Analytics (daily summaries per region)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: trending_analytics...")

trending = stats_df.groupBy("region", "trending_date_parsed").agg(
    F.count("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("dislikes").alias("total_dislikes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("like_ratio").alias("avg_like_ratio"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("max_views"),
    F.countDistinct("channel_title").alias("unique_channels"),
    F.countDistinct("category_id").alias("unique_categories"),
)

trending = trending.withColumn("_aggregated_at", F.current_timestamp())

trending_path = f"s3://{GOLD_BUCKET}/youtube/trending_analytics/"
trending_dyf = DynamicFrame.fromDF(trending, glueContext, "trending")

sink1 = glueContext.getSink(
    connection_type="s3",
    path=trending_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink1.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="trending_analytics")
sink1.setFormat("glueparquet", compression="snappy")
sink1.writeFrame(trending_dyf)
logger.info(f"  Written {trending.count()} rows → {trending_path}")

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 2: Channel Analytics
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: channel_analytics...")

channel = stats_df.groupBy("channel_title", "region").agg(
    F.countDistinct("video_id").alias("total_videos"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("views").alias("avg_views_per_video"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.max("views").alias("peak_views"),
    F.count("trending_date_parsed").alias("times_trending"),
    F.min("trending_date_parsed").alias("first_trending"),
    F.max("trending_date_parsed").alias("last_trending"),
    F.collect_set("category_name").alias("categories"),
)

# Rank channels by total views within each region
window_rank = Window.partitionBy("region").orderBy(F.col("total_views").desc())
channel = channel.withColumn("rank_in_region", F.row_number().over(window_rank))
channel = channel.withColumn("_aggregated_at", F.current_timestamp())

channel_path = f"s3://{GOLD_BUCKET}/youtube/channel_analytics/"
channel_dyf = DynamicFrame.fromDF(channel, glueContext, "channel")

sink2 = glueContext.getSink(
    connection_type="s3",
    path=channel_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink2.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="channel_analytics")
sink2.setFormat("glueparquet", compression="snappy")
sink2.writeFrame(channel_dyf)
logger.info(f"  Written {channel.count()} rows → {channel_path}")

# ══════════════════════════════════════════════════════════════════════════════
# GOLD TABLE 3: Category Analytics (trend over time)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("Building Gold: category_analytics...")

category = stats_df.groupBy("category_name", "category_id", "region", "trending_date_parsed").agg(
    F.count("video_id").alias("video_count"),
    F.sum("views").alias("total_views"),
    F.sum("likes").alias("total_likes"),
    F.sum("comment_count").alias("total_comments"),
    F.avg("engagement_rate").alias("avg_engagement_rate"),
    F.countDistinct("channel_title").alias("unique_channels"),
)

# Category share of views per region per day
window_total = Window.partitionBy("region", "trending_date_parsed")
category = category.withColumn(
    "view_share_pct",
    F.round(F.col("total_views") / F.sum("total_views").over(window_total) * 100, 2)
)
category = category.withColumn("_aggregated_at", F.current_timestamp())

category_path = f"s3://{GOLD_BUCKET}/youtube/category_analytics/"
category_dyf = DynamicFrame.fromDF(category, glueContext, "category")

sink3 = glueContext.getSink(
    connection_type="s3",
    path=category_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink3.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName="category_analytics")
sink3.setFormat("glueparquet", compression="snappy")
sink3.writeFrame(category_dyf)
logger.info(f"  Written {category.count()} rows → {category_path}")

logger.info("Gold layer build complete.")
job.commit()