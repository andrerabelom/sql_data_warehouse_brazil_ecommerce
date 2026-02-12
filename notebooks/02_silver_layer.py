# Databricks notebook source
# DBTITLE 1,Cell 1 (fixed)
# Define widgets for bronze (ingestion) and silver (treatment) paths
# Use Unity Catalog volume paths instead of public DBFS root

dbutils.widgets.text("bronze_path", "/brazilian_ecommerce/bronze/", "Bronze Path")
dbutils.widgets.text("silver_path", "/Volumes/brazilian_ecommerce/silver/", "Silver Path")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")

# Define bronze tables and their schemas (column names and target types)
bronze_tables = {
    "customers": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string",
        "ingestion_time": "timestamp"
    },
    "orders": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp"
    },
    "products": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_lenght": "int",
        "product_description_lenght": "int",
        "product_photos_qty": "int",
        "product_weight_g": "double",
        "product_length_cm": "double",
        "product_height_cm": "double",
        "product_width_cm": "double"
    },
    "order_items": {
        "order_id": "string",
        "order_item_id": "string",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "double",
        "freight_value": "double",
        "ingestion_time": "timestamp"
    },
    "payments": {
        "order_id": "string",
        "payment_sequential": "int",
        "payment_type": "string",
        "payment_installments": "int",
        "payment_value": "double",
        "ingestion_time": "timestamp"
    },
    "sellers": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller": "string",
        "shipping_limit_date": "timestamp",
        "price": "double",
        "freight_value": "double",
        "ingestion_time": "timestamp"
    }
}


# Function to cast columns to target types
def cast_columns(df, schema_dict):
    for col, dtype in schema_dict.items():
        df = df.withColumn(col, df[col].cast(dtype))
    return df



# # Process each bronze table and write to silver layer
# for table_name, schema in bronze_tables.items():
#     bronze_df = spark.table(f"{bronze_path}.{table_name}")
#     silver_df = cast_columns(bronze_df, schema)
#     silver_table_name = table_name
#     silver_df.write.format("delta").mode("overwrite").saveAsTable(f"brazilian_ecommerce.silver.{silver_table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Treatment of customers table

# COMMAND ----------

# DBTITLE 1,Silver - Customers table
from pyspark.sql import functions as F

# Check for nulls and basic statistics in bronze customers table
bronze_customers_df = spark.table(f"{bronze_path}.customers")

# Null counts per column
null_counts = bronze_customers_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze_customers_df.columns])
#display(null_counts)

# Basic statistics for numeric columns
numeric_cols = [c for c, t in bronze_tables["customers"].items() if t in ["int", "double"]]
if numeric_cols:
    stats = bronze_customers_df.select(numeric_cols).describe()
    #display(stats)


# Treatment of types
bronze_customers_df = cast_columns(bronze_customers_df, bronze_tables["customers"])

# Treatment for customer_city
bronze_customers_df = bronze_customers_df.withColumn(
    "customer_city", F.initcap(F.trim(F.col("customer_city")))
)

# Treatment for customer_state
bronze_customers_df = bronze_customers_df.withColumn(
    "customer_state", F.trim(F.col("customer_state"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Treatment of orders table

# COMMAND ----------

# DBTITLE 1,Silver - Orders Table
# Check for nulls and basic statistics in bronze orders table
bronze_orders_df = spark.table(f"{bronze_path}.orders")

# Null counts per column
null_counts = bronze_orders_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze_orders_df.columns])
#display(null_counts)


# Treatment of types
bronze_orders_df = cast_columns(bronze_orders_df, bronze_tables["orders"])

#Deduplicates by order_id. We can't have the same order_id appearing more than once.
bronze_orders_df = bronze_orders_df.dropDuplicates(["order_id"])

# Add is_delivered
bronze_orders_df = (
    bronze_orders_df
    # 1. Create flags
    .withColumn("is_delivered", F.col("order_delivered_customer_date").isNotNull())
    
    # 2. Calculate dates (Nulls are handled automatically by datediff)
    .withColumn("delivery_diff_days", 
                F.datediff("order_delivered_customer_date", "order_estimated_delivery_date").cast("int"))
    
    # 3. Time to approve
    .withColumn("approval_time_hours", 
                (F.unix_timestamp("order_approved_at") - F.unix_timestamp("order_purchase_timestamp")) / 3600)
)



# Trims all trailing spaces from "order_status"
bronze_orders_df = bronze_orders_df.withColumn(
    "order_status", F.trim(F.col("order_status"))
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Treatment of payments table

# COMMAND ----------

# DBTITLE 1,Silver - Payments table
# Check for nulls and basic statistics in bronze payments table
bronze_payments_df = spark.table(f"{bronze_path}.payments")

# Null counts per column
null_counts = bronze_payments_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze_payments_df.columns])
#display(null_counts)

# Treatment of types
bronze_payments_df = cast_columns(bronze_payments_df, bronze_tables["payments"])

# Trims all trailing spaces from "order_status"
bronze_payments_df = bronze_payments_df.withColumn(
    "payment_type", F.trim(F.col("payment_type"))
)

# Deduplicate by all columns. If the entire row is equal, leave only the first row.
bronze_payments_df = bronze_payments_df.dropDuplicates()

bronze_payments_df = bronze_payments_df.withColumn(
    "installment_sale_bool", (F.col("payment_installments") > 1)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Treatment of order_items table

# COMMAND ----------

# DBTITLE 1,Silver - Order Items dataset
# Check for nulls and basic statistics in bronze order_items table
bronze_order_items_df = spark.table(f"{bronze_path}.order_items")

# Null counts per column
null_counts = bronze_order_items_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze_order_items_df.columns])
#display(null_counts)

# Treatment of types
bronze_order_items_df = cast_columns(bronze_order_items_df, bronze_tables["order_items"])


# Creates a new column "total_cost" that sums "price" and "freight_value"
bronze_order_items_df = bronze_order_items_df.withColumn(
    "total_cost", F.coalesce(F.col("price"), F.lit(0)) + F.coalesce(F.col("freight_value"), F.lit(0))
)




# COMMAND ----------

# MAGIC %md
# MAGIC ## Treatment of products table

# COMMAND ----------

# Check for nulls and basic statistics in bronze products table
bronze_products_df = spark.table(f"{bronze_path}.products")

# Null counts per column
null_counts = bronze_products_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bronze_products_df.columns])
#display(null_counts)


# Removes trailing spaces and lowercases product_category_name
bronze_products_df = bronze_products_df.withColumn(
    "product_category_name", F.lower(F.trim(F.col("product_category_name")))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation

# COMMAND ----------

# Write treated bronze DataFrames to silver Delta tables with overwrite mode and Z-ORDER optimization

# Mapping of treated bronze DataFrames
treated_dfs = {
    "customers": bronze_customers_df,
    "orders": bronze_orders_df,
    "payments": bronze_payments_df,
    "order_items": bronze_order_items_df
}

# Recommended Z-ORDER columns for each table
cluster_keys = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "payments": ["order_id"],
    "order_items": ["order_id"]
}

for table_name, df in treated_dfs.items():
    silver_table = f"brazilian_ecommerce.silver.{table_name}"

    # Get the columns for this specific table from our dictionary
    keys = cluster_keys.get(table_name, [])
    
    # Write to Delta table in overwrite mode
    # Guarantees that there will be no errors in case of an empty list (because of clusterby)

    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    
    if keys:
        writer = writer.clusterBy(*keys)
    
    # Saves the tables
    writer.saveAsTable(silver_table)
    
    # # Optimize with Z-ORDER
    # z_cols = ", ".join(zorder_columns[table_name])
    # spark.sql(f"""
    #     OPTIMIZE {silver_table}
    #     ZORDER BY ({z_cols})
    # """)