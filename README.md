# End-to-End Real-World Data Engineering Project: Retail Sales Medallion Pipeline

This production-ready repository documents the end-to-end design, implementation, and optimization of an enterprise-grade **Databricks Medallion Pipeline**. The architecture processes high-velocity ingestion feeds (Sensor Data, Transactions, and CRM) into a structured Lakehouse environment using Spark SQL and Delta Lake engine capabilities.

## 🚀 Live Project & Interactive Presentation

Explore the architecture interactive walk-through and dashboard wireframes live on the web: 
👉 **[Live Project Presentation Interface](https://lovableproject.com)**


---

## 📌 1. Project High-Level Architecture
The end-to-end data flow ingestion pattern operates as illustrated in the architecture mapping below:

```text
  [ CRM Data ] --------\
  [ Inventory ] -------+---> [ Cloud Storage (DBFS) ] ---> [ Bronze Lakehouse ] ---> [ Silver Lakehouse ] ---> [ Gold Lakehouse ] ---> [ Analytical BI Layer ]
  [ Transactions ] ----/         Raw Ingestion               Raw Sensor/Tx Data       Cleaned & Constrained      Curated Aggregates      Power BI Dashboard
```

* **Data Source Ingestion:** Integration pipelines pulling from source systems containing business assets (`CRM`, `Inventory`, `Transactions`).
* **Deployment & Monitoring:** Controlled via an orchestrator (**Azure Data Factory**), code deployments (**Git CI/CD pipelines**), and **Data Quality Monitoring** constraints.
* **Orchestration & Governance:** Features **Metadata management** and **Security layer rules** such as strict data masking protocols for sensitive data assets.

---

## 📅 2. Implementation Schedule & Roadmap
The core system implementation follows a two-phase delivery lifecycle structure:

* **Phase 1: Canva Design & Brainstorming**
  * Brainstorming core Medallion architecture components.
  * Selecting performance metrics and infrastructure configuration grabs.
  * Mapping out the sequential pipeline execution workflow.
* **Phase 2: Implementation Schedule**
  * **Azure Databricks Setup:** Base cluster configuration and compute node definitions.
  * **Data Ingestion:** landing `.csv` raw object structures into file stores.
  * **Bronze Layer Delta Tables:** Writing schemas directly from active ingestion paths.
  * **Silver Layer Cleaning:** Applying data standardizations, data type casting, and anomaly filtering rules.
  * **Gold Layer Business Aggregates:** Building dimensional modeling frameworks (`gold_dim_customer`, `gold_fact_sales`) ready for presentation connection.

---

## 🛠️ 3. Environment & Cluster Configuration (Step 1)
To process the workloads efficiently without over-allocating operational budgets, the development execution environment uses automated scaling parameters inside the Databricks cluster interface:

* **Cluster Name:** `DS_DEV_CLUSTER`
* **Databricks Runtime Version:** `13.3 LTS` (Long Term Support)
* **Worker Node Type:** `Standard_DS3_v2` virtual machines
* **Auto-Scaling Parameters:** Dynamically scales between a minimum of **1 worker** and a maximum of **8 workers**.
* **Cost Optimization Strategy:** Configured to automatically terminate after **30 minutes of inactivity** to prevent orphaned compute spend.

---

## 📥 4. Ingestion & Bronze Layer Schema (Step 2)
Raw sensory and transactional feeds land systematically into the Databricks workspace browser catalog. Data structures drop incrementally as timestamped `.csv` file formats under explicit logical partitions:

### File Directory Management
```text
Catalog / Bronze / Raw_Ingestion /
├── raw_sensor_data/
│   ├── sensor_data_20231026.csv
│   ├── sensor_data_20231025.csv
└── raw_transaction_data/
    ├── sensor_data_20231025.csv
```

### Raw Target Ingestion Schema (`bronze.raw_sensor_table`)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `name` | **STRING** | System identifier string |
| `sensor_id` | **INTEGER** | Unique identification number for sensor devices |
| `timestamp` | **TIMESTAMP** | Precise tracking execution time marker |
| `temperature`| **FLOAT** | Environmental thermal metric |
| `pressure` | **FLOAT** | Environmental atmospheric metric |

---

## 🧹 5. Data Cleansing & Validation Layer (Step 3)
The **Silver Layer** handles downstream reliability transformations. It structures logic safely using Spark SQL temporary views, converts data schemas into precise formats, handles null records, and applies stateful **Delta Lake Constraints**.

```sql
-- STEP 3.1: Transformation & Constraint Definition
-- CLEAN & CAST DATA FROM BRONZE
CREATE OR REPLACE TEMPORARY VIEW raw_events_cleaned AS
SELECT
    CAST(event_id AS STRING) AS event_id,
    CAST(user_id AS STRING) AS user_id,
    CAST(timestamp AS TIMESTAMP) AS event_time,
    LOWER(trim(event_type)) AS event_category,
    COALESCE(value_amount, 0.0) AS event_value
FROM silver_raw_sensor_table
WHERE event_id IS NOT NULL; -- Initial basic filter

-- DEFINE & ENFORCE CONSTRAINTS ON SILVER TABLE
CREATE TABLE IF NOT EXISTS gold_clean_validated (
    event_id STRING NOT NULL, -- Essential Primary Key
    user_id STRING NOT NULL,
    event_time TIMESTAMP NOT NULL,
    event_category STRING NOT NULL
) USING DELTA;

-- Add a CHECK constraint for value consistency
ALTER TABLE gold_clean_validated ADD CONSTRAINT check_event_value_positive CHECK (event_value >= 0.0);

-- Add a CHECK constraint for category validity
ALTER TABLE gold_clean_validated ADD CONSTRAINT check_valid_category CHECK (event_category IN ('view', 'click', 'purchase'));

-- UPSERT Cleaned Data into the Constraint-Enforced Table
MERGE INTO gold_clean_validated AS target
USING raw_events_cleaned AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN
    UPDATE SET target.event_value = source.event_value, target.event_time = source.event_time
WHEN NOT MATCHED THEN
    INSERT *;
```

---

## 📊 6. Gold Layer Dimensional Serving Logic
The analytical layer transforms clean tables into clean **Star Schemas** running on optimized **SQL Warehouses**. The storage objects expose explicit facts and dimension definitions optimized for high-speed DirectQuery analytics inside Power BI.

### Gold Data Models Created
* `gold_dim_customer`: Contains explicit metrics such as `customer_id`, `full_name`, `total_spend_dim`, and calculated custom metrics like `lifetime_value_calc`.
* `gold_fact_sales`: Handles transactional aggregation boundaries including `gfs.sale_date`, `gdp.product_category`, and `gfs.product_id` intersections.

### SQL Serving Guidelines & Ad-Hoc Analytics Workload
```sql
--- ISOLATED SQL WAREHOUSE SERVING ---
USE gold_aggregated;

-- Query 1: Find high-value customers
SELECT 
    customer_id,
    full_name,
    total_spend_dim,
    lifetime_value_calc
FROM 
    gold_dim_customer
WHERE 
    lifetime_value_calc > 50000
ORDER BY 
    lifetime_value_calc DESC
LIMIT 10;

-- Query 2: Daily sales summary with products
SELECT 
    gfs.sale_date,
    gdp.product_category,
    SUM(gfs.sale_amount) AS total_category_sales
FROM 
    gold_fact_sales AS gfs
JOIN 
    gold_dim_product AS gdp
ON 
    gfs.product_id = gdp.product_id
GROUP BY 
    gfs.sale_date, gdp.product_category
HAVING 
    SUM(gfs.sale_amount) > 1000
ORDER BY 
    gfs.sale_date DESC, total_category_sales DESC;
```
