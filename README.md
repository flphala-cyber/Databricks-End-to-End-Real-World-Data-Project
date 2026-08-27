# End-to-End Retail Sales Data Engineering Project

A production-grade Azure Databricks pipeline that ingests raw POS data, cleans and conforms it through the **Medallion Architecture**, and serves a star-schema analytical model to **Power BI**.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Medallion Architecture](#medallion-architecture)
  - [Bronze — Raw Ingestion](#bronze--raw-ingestion)
  - [Silver — Cleansed & Conformed](#silver--cleansed--conformed)
  - [Gold — Business-Ready Aggregates](#gold--business-ready-aggregates)
- [Cluster Configuration](#cluster-configuration)
- [Data Cleansing & Validation](#data-cleansing--validation)
- [Dimensional Star-Schema Modeling](#dimensional-star-schema-modeling)
- [Power BI Integration](#power-bi-integration)
- [Resources & Takeaways](#resources--takeaways)

---

## Architecture Overview

The pipeline follows a three-tier lakehouse pattern. Data lands in its raw form, is progressively refined and validated, and finally shaped into a dimensional model optimized for BI and analytics workloads.

```text
Raw POS / CSV / API feeds
       |
       ▼
┌─────────────────────────────────────┐
│  Bronze Layer (Data Lake / ADLS)    │  ◄── Raw, immutable, schema-on-read
└─────────────────────────────────────┘
       |
       ▼  PySpark ETL + Delta Lake
┌─────────────────────────────────────┐
│  Silver Layer (Cleansed & Conformed)│  ◄── Strong types, deduplication, SCD2
└─────────────────────────────────────┘
       |
       ▼  Aggregations + Dimensional joins
┌─────────────────────────────────────┐
│  Gold Layer (Star Schema)           │  ◄── Business-ready facts & dimensions
└─────────────────────────────────────┘
       |
       ▼  Power BI DirectQuery / Import
┌─────────────────────────────────────┐
│  Dashboards & Reports               │
└─────────────────────────────────────┘

from pyspark.sql.functions import current_timestamp, lit, input_file_name

# Ingest raw CSV from ADLS Gen2
raw_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load("/mnt/bronze/retail/sales/raw/"))

# Add metadata columns for lineage
bronze_df = (raw_df
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("layer", lit("bronze")))

# Write once as a Delta table with append semantics
bronze_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("retail.bronze.sales_transactions")


from pyspark.sql.functions import (
    col, trim, lower, to_date, regexp_replace
)
from pyspark.sql.types import DecimalType

silver_df = (spark.table("retail.bronze.sales_transactions")
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price",
        regexp_replace(col("unit_price"), r"[$,]", "").cast(DecimalType(10, 2)))
    .withColumn("discount_amount",
        regexp_replace(col("discount_amount"), r"[$,]", "").cast(DecimalType(10, 2)))
    .withColumn("product_name", trim(lower(col("product_name"))))
    .withColumn("store_code", trim(col("store_code")))
    .filter(col("transaction_id").isNotNull())
    .dropDuplicates(["transaction_id"]))

silver_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail.silver.sales_transactions")


fact_sales = (spark.table("retail.silver.sales_transactions")
    .join(spark.table("retail.gold.dim_date"), "date_key")
    .join(spark.table("retail.gold.dim_customer"), "customer_key")
    .join(spark.table("retail.gold.dim_product"), "product_key")
    .join(spark.table("retail.gold.dim_store"), "store_key")
    .select(
        col("transaction_id"),
        col("date_key"),
        col("customer_key"),
        col("product_key"),
        col("store_key"),
        col("quantity"),
        col("unit_price"),
        col("discount_amount"),
        (col("quantity") * col("unit_price") - col("discount_amount"))
            .alias("sales_amount"),
        (col("quantity") * col("unit_cost")).alias("cost_amount"),
        (col("sales_amount") - col("cost_amount")).alias("profit_amount")
    ))

fact_sales.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.gold.fact_sales")


{
  "cluster_name": "DS_DEV_CLUSTER",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autotermination_minutes": 30,
  "autoscale": {
    "min_workers": 1,
    "max_workers": 4
  },
  "data_security_mode": "SINGLE_USER",
  "runtime_engine": "STANDARD",
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
  }
}


from pyspark.sql.functions import col

silver_df = spark.table("retail.silver.sales_transactions")

checks = [
    ("transaction_id is not null", silver_df.filter(col("transaction_id").isNull()).count() == 0),
    ("quantity is positive", silver_df.filter(col("quantity") <= 0).count() == 0),
    ("unit_price is non-negative", silver_df.filter(col("unit_price") < 0).count() == 0),
    ("transaction_date is valid", silver_df.filter(col("transaction_date").isNull()).count() == 0),
]

failed = [name for name, passed in checks if not passed]
if failed:
    raise ValueError(f"Data quality checks failed: {failed}")
else:
    print("All data quality checks passed.")


                    ┌─────────────────┐
                    │   dim_date      │
                    │  (date_key PK)  │
                    └────────┬────────┘
                             │
┌─────────────┐    ┌───────┴────────┐    ┌─────────────┐
│ dim_customer│    │    fact_sales    │    │  dim_store  │
│(customer_key)│   │  (sale_key PK)   │    │  (store_key)│
└─────────────┘    │  transaction_id  │    └─────────────┘
                   │  date_key (FK)   │
                   │  customer_key(FK) │
                   │  product_key (FK) │
                   │  store_key  (FK)  │
                   │  quantity        │
                   │  unit_price      │
                   │  discount_amount │
                   │  sales_amount    │
                   │  cost_amount     │
                   │  profit_amount   │
                   └───────┬──────────┘
                           │
                    ┌──────┴────────┐
                    │   dim_product   │
                    │ (product_key)   │
                    └─────────────────┘


CREATE TABLE IF NOT EXISTS retail.gold.fact_sales (
  sale_key        BIGINT,
  transaction_id  STRING  NOT NULL,
  date_key        INT     NOT NULL,
  customer_key    INT     NOT NULL,
  product_key     INT     NOT NULL,
  store_key       INT     NOT NULL,
  quantity        INT     NOT NULL,
  unit_price      DECIMAL(10,2) NOT NULL,
  discount_amount DECIMAL(10,2),
  sales_amount    DECIMAL(12,2) NOT NULL,
  cost_amount     DECIMAL(12,2) NOT NULL,
  profit_amount   DECIMAL(12,2) NOT NULL
)
USING DELTA
PARTITIONED BY (date_key);

Total Sales = SUM(fact_sales[sales_amount])
Total Cost = SUM(fact_sales[cost_amount])
Total Profit = SUM(fact_sales[profit_amount])
Profit Margin = DIVIDE([Total Profit], [Total Sales], 0)
Sales YTD = TOTALYTD([Total Sales], dim_date[full_date])



