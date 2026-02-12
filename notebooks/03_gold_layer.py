# Databricks notebook source
order_df = spark.table("brazilian_ecommerce.silver.orders")

order_items_df = spark.table("brazilian_ecommerce.silver.order_items").select(
    "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "freight_value", "price", "total_cost"
)
customers_df = spark.table("brazilian_ecommerce.silver.customers").select(
    "customer_id", "customer_unique_id", "customer_city", "customer_state"
)
payments_df = spark.table("brazilian_ecommerce.gold.ecommerce_project_gold_agg_payments").select(
    "order_id", "total_payment_value", "max_payment_installments", "first_occuring_payment_type"
)

# Left join orders with order_items, customers, and payments
fact_orders_df = (
    order_df
    .join(order_items_df, on="order_id", how="left")
    .join(customers_df, on="customer_id", how="left")
    .join(payments_df, on="order_id", how="left")
)


# Feature Engineering
from pyspark.sql import functions as F

# Add the ghost_sale flag
fact_orders_df = fact_orders_df.withColumn(
    "is_ghost_sale", 
    # If there's no product_id, it's a ghost sale (True), otherwise False
    F.when(F.col("product_id").isNull(), True).otherwise(False)
)

# Rename total_cost to item_gross_revenue
fact_orders_df = fact_orders_df.withColumnRenamed("total_cost", "item_gross_revenue")


# DBTITLE 1,Create Fact Table
# Create the fact table
fact_orders_df.write.mode("overwrite").option("overwriteSchema", "true").clusterBy("order_id", "customer_id").saveAsTable("brazilian_ecommerce.gold.fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check for payment values

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RECONCILIATION QUERY
# MAGIC SELECT 
# MAGIC     'Silver Payments' AS Source, 
# MAGIC     ROUND(SUM(payment_value), 2) AS Total_Value 
# MAGIC FROM brazilian_ecommerce.silver.payments
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Gold Fact Table' AS Source, 
# MAGIC     ROUND(SUM(total_payment_value / items_per_order), 2) AS Total_Value
# MAGIC FROM (
# MAGIC     -- We divide by items_per_order because payment_value is repeated for every item in an order
# MAGIC     SELECT 
# MAGIC         order_id, 
# MAGIC         total_payment_value, 
# MAGIC         COUNT(*) OVER(PARTITION BY order_id) as items_per_order
# MAGIC     FROM brazilian_ecommerce.gold.fact_orders
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Granularity Test

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should return 0 rows if grain is correct
# MAGIC SELECT order_id, order_item_id, COUNT(*)
# MAGIC FROM brazilian_ecommerce.gold.fact_orders
# MAGIC GROUP BY ALL
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer info check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(customer_unique_id) AS rows_with_customer,
# MAGIC     (COUNT(*) - COUNT(customer_unique_id)) AS orphan_records
# MAGIC FROM brazilian_ecommerce.gold.fact_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integrity Test

# COMMAND ----------

# DBTITLE 1,Data Quality Gate
def validate_gold_table(table_name):
    # Check 1: Row Count
    count = spark.table(table_name).count()
    assert count > 0, f"Error: {table_name} is empty!"
    
    # Check 2: Nulls in Primary Keys
    null_count = spark.table(table_name).filter("order_id IS NULL OR product_id IS NULL").count()
    assert null_count == 0, f"Error: {table_name} contains NULL IDs!"
    
    print(f"✅ Data Quality Audit Passed for {table_name} ({count} rows verified).")

validate_gold_table("brazilian_ecommerce.gold.fact_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     order_status, 
# MAGIC     COUNT(*) as total_orders,
# MAGIC     COUNT(product_id) as orders_with_products,
# MAGIC     COUNT(*) - COUNT(product_id) as missing_products
# MAGIC FROM brazilian_ecommerce.gold.fact_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY missing_products DESC