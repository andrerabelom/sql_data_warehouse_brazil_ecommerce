# Databricks notebook source
# Databricks widgets for dynamic catalog and schema selection
dbutils.widgets.text("catalog", "brazilian_ecommerce", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql.functions import current_timestamp
import os



folders = [
    "orders",
    "customers",
    "order_items",
    "products",
    "geolocation",
    "payments",
    "reviews",
    "sellers"
]

for folder in folders:
    input_path = f"/Volumes/{catalog}/{schema}/landing_zone/{folder}/"
    schema_location = f"/Volumes/{catalog}/{schema}/landing_zone/{folder}/{folder}_schema"
    checkpoint_location = f"/Volumes/{catalog}/{schema}/_metadata/{folder}_checkpoint"
    table_name = f"{catalog}.{schema}.{folder}"


    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferSchema", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")  # Read CSV headers
        .load(input_path)
    )

    df_with_metadata = df.withColumn("ingestion_time", current_timestamp())

    (
        df_with_metadata.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table_name)
    )