# Databricks notebook source
# DBTITLE 1,Create gold schema
# MAGIC %sql
# MAGIC create schema if not exists workspace.gold

# COMMAND ----------

# DBTITLE 1,Create gold refresh log table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.gold.gold_refresh_log (
# MAGIC     table_name STRING,
# MAGIC     last_refreshed_at TIMESTAMP,
# MAGIC     silver_max_last_updated TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Helpers: incremental refresh tracking
# MAGIC %md
# MAGIC ### Helpers: incremental refresh tracking

# COMMAND ----------

# DBTITLE 1,Import and define constants
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

SILVER_TABLE = "workspace.silver.world_bank_gdp"
REFRESH_LOG_TABLE = "workspace.gold.gold_refresh_log"

# COMMAND ----------

# DBTITLE 1,Refresh helper functions
def get_silver_max_ts():
    """Latest ingestion_timestamp currently in silver."""
    row = spark.table(SILVER_TABLE).agg(F.max("ingestion_timestamp").alias("max_ts")).collect()[0]
    return row["max_ts"]


def get_last_refreshed_silver_ts(gold_table_name):
    """Silver max timestamp as of the last successful refresh of this gold table, or None if never refreshed."""
    log_df = spark.table(REFRESH_LOG_TABLE).filter(F.col("table_name") == gold_table_name)
    if log_df.limit(1).count() == 0:
        return None
    return log_df.agg(F.max("silver_max_last_updated").alias("ts")).collect()[0]["ts"]


def needs_refresh(gold_table_name):
    """Returns (should_refresh: bool, silver_max_ts) by comparing silver's latest data
    to what this gold table was last built from."""
    silver_max_ts = get_silver_max_ts()
    if silver_max_ts is None:
        # silver is empty, nothing to build from
        return False, None

    last_refreshed_ts = get_last_refreshed_silver_ts(gold_table_name)
    if last_refreshed_ts is None or silver_max_ts > last_refreshed_ts:
        return True, silver_max_ts
    return False, silver_max_ts


def log_refresh(gold_table_name, silver_max_ts):
    """Append a row recording that this gold table was refreshed from silver as of silver_max_ts."""
    log_row = spark.createDataFrame(
        [(gold_table_name, datetime.datetime.now(), silver_max_ts)],
        "table_name STRING, last_refreshed_at TIMESTAMP, silver_max_last_updated TIMESTAMP"
    )
    log_row.write.format("delta").mode("append").saveAsTable(REFRESH_LOG_TABLE)

# COMMAND ----------

# DBTITLE 1,Gold table 1: gdp_by_country_year
# MAGIC %md
# MAGIC ### Gold table 1: `gdp_by_country_year`
# MAGIC Total GDP per country/year, plus that country's GDP rank among all countries in that year.

# COMMAND ----------

# DBTITLE 1,Build gdp_by_country_year
GOLD_TABLE_1 = "workspace.gold.gdp_by_country_year"


def build_gdp_by_country_year():
    df = spark.table(SILVER_TABLE)

    gold_df = (
        df
        .groupBy("country_code", "country_name", "countryiso3code", "year")
        .agg(F.sum("gdp_value").alias("gdp_value"))
    )

    w = Window.partitionBy("year").orderBy(F.col("gdp_value").desc())
    gold_df = gold_df.withColumn("gdp_rank_in_year", F.rank().over(w))

    gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_1)


should_refresh, silver_max_ts = needs_refresh(GOLD_TABLE_1)

if should_refresh:
    build_gdp_by_country_year()
    log_refresh(GOLD_TABLE_1, silver_max_ts)
    print(f"Refreshed {GOLD_TABLE_1}")
else:
    print(f"{GOLD_TABLE_1} already up to date")

# COMMAND ----------

# DBTITLE 1,Gold table 2: gdp_yoy_growth
# MAGIC %md
# MAGIC ### Gold table 2: `gdp_yoy_growth`
# MAGIC GDP and year-over-year % growth per country/indicator/year.

# COMMAND ----------

# DBTITLE 1,Build gdp_yoy_growth
GOLD_TABLE_2 = "workspace.gold.gdp_yoy_growth"


def build_gdp_yoy_growth():
    df = spark.table(SILVER_TABLE)

    w = Window.partitionBy("country_code", "indicator_code").orderBy("year")

    gold_df = (
        df
        .withColumn("prev_year_gdp", F.lag("gdp_value").over(w))
        .withColumn(
            "yoy_growth_pct",
            F.when(
                (F.col("prev_year_gdp").isNotNull()) & (F.col("prev_year_gdp") != 0),
                (F.col("gdp_value") - F.col("prev_year_gdp")) / F.col("prev_year_gdp") * 100
            )
        )
        .select(
            "country_code", "country_name", "countryiso3code",
            "indicator_code", "indicator_name", "year",
            "gdp_value", "prev_year_gdp", "yoy_growth_pct"
        )
    )

    gold_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_TABLE_2)


should_refresh, silver_max_ts = needs_refresh(GOLD_TABLE_2)

if should_refresh:
    build_gdp_yoy_growth()
    log_refresh(GOLD_TABLE_2, silver_max_ts)
    print(f"Refreshed {GOLD_TABLE_2}")
else:
    print(f"{GOLD_TABLE_2} already up to date")

# COMMAND ----------

# DBTITLE 1,Sanity check
# MAGIC %md
# MAGIC ### Sanity check

# COMMAND ----------

# DBTITLE 1,Display gdp_by_country_year
display(spark.table(GOLD_TABLE_1).orderBy("year", "gdp_rank_in_year"))

# COMMAND ----------

# DBTITLE 1,Display gdp_yoy_growth
display(spark.table(GOLD_TABLE_2).orderBy("country_code", "year"))

# COMMAND ----------

