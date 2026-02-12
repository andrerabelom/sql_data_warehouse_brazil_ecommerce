# Olist Brazilian E-commerce: Medallion Architecture
**End-to-End Data Engineering Project | Databricks | PySpark | Delta Lake | Unity Catalog**

## Project Objective
To build a robust, scalable data pipeline that transforms raw Brazilian e-commerce data (Olist) into an audited, high-performance **Star Schema** (Gold Layer). The project focuses on handling complex order lifecycles, ensuring financial reconciliation, and implementing modern storage optimizations.

---

##  The Architecture: Medallion Pattern
I implemented a 3-tier architecture within **Databricks Unity Catalog**:

* **Bronze (Raw):** Ingested CSV files using Auto Loader, preserving raw state with added `ingestion_time` metadata.
* **Silver (Cleansed):** * **Schema Enforcement:** Explicitly cast columns to Timestamps, Integers, and Doubles.
    * **Data Quality:** Sanitized strings (trimming, proper casing) and handled NULL values.
    * **Deduplication:** Applied logic to ensure unique primary keys while maintaining historical integrity.
* **Gold (Curated):** * **Fact Table:** Designed `fact_orders` at the **Line-Item Grain** for deep analytical granularity.
    * **Optimization:** Implemented **Liquid Clustering** on `order_id` and `customer_id` for O(1) query performance.

---

##  Environment Setup & Reproducibility

### 1. Folder Structure
The notebooks are designed to be run in sequence. Maintain this hierarchy in your Databricks Workspace:
* `/Users/<email>/ecommerce_project/`
    * `01_bronze_ingestion`
    * `02_silver_transformations`
    * `03_gold_agg_payments` (Creates the payment aggregation view)
    * `04_gold_fact_orders` (Main Fact creation & DQ Audits)

### 2. Unity Catalog Requirements
This project uses **Three-Level Namespacing** (`catalog.schema.table`). 
* **Catalog:** `brazilian_ecommerce`
* **Schemas:** `bronze`, `silver`, and `gold`

### 3. Data Source
* **Dataset:** [Kaggle Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
* **Setup:** Upload CSVs to a Unity Catalog Volume and update the `bronze_path` widget in Notebook 01.

---

##  Key Engineering Challenges & Solutions

### 1. The "Ghost Sale" Phenomenon
**Challenge:** During joins, 0.5% of orders lacked product details (statuses like `canceled` or `unavailable`).
**Solution:** Utilized a `LEFT JOIN` to preserve these records and engineered an `is_ghost_sale` boolean flag. This allows analysts to filter for real revenue while providing Operations teams visibility into abandoned/failed conversion intent.

### 2. Financial Reconciliation ($77k Recovery)
**Challenge:** Initial aggregations were losing ~$77,000 in revenue due to improper `ROW_NUMBER()` filtering on orders with multiple payment methods (e.g., Credit Card + Voucher).
**Solution:** Redesigned the Payment Aggregation using **Window Functions** (`SUM OVER PARTITION`) to ensure 100% of payment value was captured before selecting a single representative row for payment metadata.

### 3. Data Quality (DQ) Gates
Automated validation checks are integrated at the end of the pipeline:
* **Reconciliation:** Verified that Gold `total_payment` balances against Silver `payment_value`.
* **Integrity:** Confirmed 100% of 'Delivered' orders contain associated products.
* **Uniqueness:** Validated grain integrity (Order ID + Item ID).

---

##  Data Dictionary (Gold Layer)

| Column | Type | Description |
| :--- | :--- | :--- |
| `order_id` | String | Unique identifier for the order. |
| `item_gross_revenue` | Double | Calculated: `price` + `freight_value`. |
| `total_payment_value` | Double | Reconciled sum of all payment methods for the order. |
| `is_ghost_sale` | Boolean | True if order exists in 'Orders' but has no items in 'Order_Items'. |
| `delivery_diff_days` | Integer | Days between estimated and actual delivery (Silver engineered). |

---

##  Sample Insights Generated
* **Logistics:** Identified specific states where `delivery_diff_days` consistently exceeded estimates.
* **Revenue:** Real-time Gross Revenue calculation per Brazilian State and Product Category.
* **Payment Strategy:** Analysis of installment preferences versus total order value.
