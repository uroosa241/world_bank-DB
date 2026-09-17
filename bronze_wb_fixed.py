# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### 1. Setup: paths + directories

# COMMAND ----------

# small update

# COMMAND ----------

# DBTITLE 1,Setup paths and directories
from pyspark.sql import functions as F

# Paths
schema_path = "/Volumes/workspace/default/raw/_schema/world_bank"
checkpoint_path = "/Volumes/workspace/default/raw/_checkpoint/world_bank"
source_path = "/Volumes/workspace/default/raw/world_bank"

# Create directories
dbutils.fs.mkdirs(schema_path)
dbutils.fs.mkdirs(checkpoint_path)
dbutils.fs.mkdirs(source_path)

print("Directories created successfully!")

# COMMAND ----------

# DBTITLE 1,Fetch from World Bank API
# MAGIC %md
# MAGIC ### 2. Fetch from the World Bank API and land raw JSON into the volume
# MAGIC
# MAGIC This was the missing step: the stream in the old notebook had nowhere to read from because nothing ever wrote files into `source_path`. We fetch here and write as **JSON Lines** (one record per line) — Auto Loader's `cloudFiles.format=json` reads newline-delimited JSON, not a single top-level JSON array.

# COMMAND ----------

# DBTITLE 1,Fetch and write raw JSON
import requests
import json as pyjson
from datetime import datetime, timezone
import uuid

url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD"

params = {
    "format": "json",
    "per_page": 1000,
    "page": 1
}

response = requests.get(url, params=params, timeout=30)
response.raise_for_status()

data = response.json()
metadata = data[0]
records = data[1]

print("Total records:", metadata["total"])
print("Total pages:", metadata["pages"])

# --- add ingestion metadata + normalize decimal type ---
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc).isoformat()

for r in records:
    r["_ingestion_timestamp"] = ingestion_time
    r["_source"] = "worldbank_api"
    r["_indicator"] = "NY.GDP.MKTP.CD"
    r["_batch_id"] = batch_id
    # r.get("decimal") is None for many rows -- keep it a real null,
    # not the literal string "None"
    r["decimal"] = str(r["decimal"]) if r.get("decimal") is not None else None

# --- write as JSON Lines into the Volume Auto Loader will watch ---
raw_file_path = f"{source_path}/world_bank_gdp_{batch_id}.json"

with open(raw_file_path, "w") as f:
    for r in records:
        f.write(pyjson.dumps(r) + "\n")

print(f"Wrote {len(records)} records to {raw_file_path}")

# COMMAND ----------

# DBTITLE 1,Confirm files landed
# MAGIC %md
# MAGIC ### 3. Confirm files landed before starting the stream

# COMMAND ----------

# DBTITLE 1,List source files
display(dbutils.fs.ls(source_path))

# COMMAND ----------

# DBTITLE 1,Create bronze schema
# MAGIC %md
# MAGIC ### 4. Create the bronze schema

# COMMAND ----------

# DBTITLE 1,Create bronze schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;

# COMMAND ----------

# DBTITLE 1,Auto Loader stream to Delta
# MAGIC %md
# MAGIC ### 5. Auto Loader stream -> Delta bronze table

# COMMAND ----------

# DBTITLE 1,Auto Loader to bronze Delta table
from pyspark.sql import functions as F

target_table = "workspace.bronze.world_bank_gdp"

# Creating Auto Loader
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path)
)

# Add metadata columns
bronze_stream = (
    bronze_stream
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source", F.lit("world_bank"))
)

# Creating Bronze Delta Table
query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
print(f"Bronze table '{target_table}' written successfully.")

# COMMAND ----------

# DBTITLE 1,Sanity check
# MAGIC %md
# MAGIC ### 6. Sanity check

# COMMAND ----------

# DBTITLE 1,Verify bronze table
display(spark.table(target_table).limit(10))
print("Row count:", spark.table(target_table).count())

# COMMAND ----------

