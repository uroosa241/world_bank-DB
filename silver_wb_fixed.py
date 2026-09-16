# Databricks notebook source
# DBTITLE 1,Setup silver paths and tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Silver checkpoint path
silver_checkpoint_path = "/Volumes/workspace/default/raw/_checkpoint/world_bank_silver"

# Tables
silver_table = "workspace.silver.world_bank_gdp"
quarantine_table = "workspace.silver.world_bank_gdp_quarantine"
bronze_table = "workspace.bronze.world_bank_gdp"

# Create checkpoint directory
dbutils.fs.mkdirs(silver_checkpoint_path)

print("silver_ready")

# COMMAND ----------

# DBTITLE 1,Create silver schema
# MAGIC %sql
# MAGIC create schema if not exists workspace.silver;

# COMMAND ----------

# DBTITLE 1,Recreate empty silver tables
# Cell 4 (recreate empty tables)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

silver_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("countryiso3code", StringType(), True),
    StructField("indicator_code", StringType(), True),
    StructField("indicator_name", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("gdp_value", DoubleType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_ingestion_timestamp", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("dq_flag", StringType(), True)
])

empty_df = spark.createDataFrame([], silver_schema)
empty_df.filter("dq_flag = 'ok'").drop("dq_flag").write.format("delta").mode("ignore").saveAsTable("workspace.silver.world_bank_gdp")
empty_df.write.format("delta").mode("ignore").saveAsTable("workspace.silver.world_bank_gdp_quarantine")

print("Silver and quarantine tables ready")

# COMMAND ----------

# DBTITLE 1,Print silver schema
df = spark.table("workspace.silver.world_bank_gdp")
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Trim string columns
########trim all string cols

from pyspark.sql import functions as F

from pyspark.sql import functions as F

trimmed_df = df

for col_name, dtype in df.dtypes:
    if dtype == "string":
        trimmed_df = trimmed_df.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )

# COMMAND ----------

# DBTITLE 1,Cast column types
####types proper

casted_df = trimmed_df.select(F.col("_batch_id").cast("string"),
                              F.col("_ingestion_timestamp").cast("string"),
                              F.col("country_code").cast("string"),
                              F.col("country_name").cast("string"),
                              F.col("countryiso3code").cast("string"),
                              F.col("indicator_code").cast("string"),
                              F.col("indicator_name").cast("string"),
                              F.col("year").cast("integer"),
                              F.col("gdp_value").cast("double"),
                              F.col("ingestion_timestamp").cast("timestamp"))

# COMMAND ----------

# DBTITLE 1,Deduplicate rows
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = (
    Window
    .partitionBy(
        "country_code",
        "indicator_code",
        "year"
    )
    .orderBy(
        F.col("_ingestion_timestamp").desc()
    )
)

deduped_df = (
    casted_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Merge into silver table
from delta.tables import DeltaTable

silver_table_name = "workspace.silver.world_bank_gdp"

if spark.catalog.tableExists(silver_table_name):

    silver_table = DeltaTable.forName(
        spark,
        silver_table_name
    )

    (
        silver_table.alias("target")
        .merge(
            deduped_df.alias("source"),
            """
            target.country_code = source.country_code
            AND target.indicator_code = source.indicator_code
            AND target.year = source.year
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# DBTITLE 1,Show df columns
df.columns

# COMMAND ----------

# DBTITLE 1,Read bronze stream
bronze_stream = (
    spark.readStream
    .table("workspace.bronze.world_bank_gdp")
)

# COMMAND ----------

# DBTITLE 1,Process batch function
def process_batch(batch_df, batch_id):

    # 0. Parse raw bronze fields into the proper silver columns.
    #    country / indicator arrive as JSON strings from the World Bank API;
    #    date and value arrive as strings and need casting.
    parsed_df = (
        batch_df
        .withColumn("country_code", F.get_json_object(F.col("country"), "$.id"))
        .withColumn("country_name", F.get_json_object(F.col("country"), "$.value"))
        .withColumn(
            "indicator_code",
            F.coalesce(F.col("_indicator"), F.get_json_object(F.col("indicator"), "$.id"))
        )
        .withColumn("indicator_name", F.get_json_object(F.col("indicator"), "$.value"))
        .withColumn("year", F.col("date").cast("int"))
        .withColumn("gdp_value", F.col("value").cast("double"))
        .select(
            "country_code", "country_name", "countryiso3code",
            "indicator_code", "indicator_name", "year", "gdp_value",
            "_batch_id", "_ingestion_timestamp", "ingestion_timestamp"
        )
    )

    # 1. Trim all string columns
    trimmed_df = parsed_df

    for col_name, dtype in parsed_df.dtypes:
        if dtype == "string":
            trimmed_df = trimmed_df.withColumn(
                col_name,
                F.trim(F.col(col_name))
            )

    # 2. Deduplicate
    w = (
        Window
        .partitionBy(
            "country_code",
            "indicator_code",
            "year"
        )
        .orderBy(
            F.col("_ingestion_timestamp").desc()
        )
    )

    deduped_df = (
        trimmed_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # 3. Create/get Silver table
    silver_table_name = "workspace.silver.world_bank_gdp"

    if spark.catalog.tableExists(silver_table_name):

        silver_table = DeltaTable.forName(
            spark,
            silver_table_name
        )

        # 4. MERGE into Silver
        (
            silver_table.alias("target")
            .merge(
                deduped_df.alias("source"),
                """
                target.country_code = source.country_code
                AND target.indicator_code = source.indicator_code
                AND target.year = source.year
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        # First run: create Silver table
        (
            deduped_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table_name)
        )

# COMMAND ----------

# DBTITLE 1,Clear checkpoint
# Clear the checkpoint so the stream reprocesses everything from scratch.
# Needed because an earlier run used a broken process_batch (see cell above)
# and the checkpoint already marked that batch as "done" even though nothing was written.
dbutils.fs.rm(silver_checkpoint_path, recurse=True)
dbutils.fs.mkdirs(silver_checkpoint_path)
print("Checkpoint cleared - next run will reprocess all of bronze")

# COMMAND ----------

# DBTITLE 1,Run silver stream
############runing stream


query = (
    bronze_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

