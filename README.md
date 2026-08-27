# End-to-End Real-World Data Engineering Project: Retail Sales Medallion Pipeline

This repository outlines a Databricks Medallion Pipeline, processing sensor and CRM data into a structured Lakehouse using Spark SQL and Delta Lake.

## 🚀 Live Project & Interactive Presentation

Explore the architecture interactive walk-through and dashboard wireframes live on the web: 
👉 **[Live Project Presentation Interface](https://lovableproject.com)**

---

## 📌 1. Project High-Level Architecture

The system ingest data from source systems (CRM, Inventory, Transactions), processing it through Bronze, Silver, and Gold layers using Azure Data Factory, Git, and Delta Lake.

---

## 📅 2. Implementation Schedule & Roadmap

- **Phase 1: Design** - Architecture planning, metric selection, and workflow mapping.
- **Phase 2: Implementation** - Databricks setup, ingestion, data cleaning, and creating Gold layer dimensional models.

---

## 🛠 3. Environment & Cluster Configuration

*   **Cluster Name:** DS_DEV_CLUSTER
*   **Runtime:** 13.3 LTS
*   **Nodes:** Standard_DS3_v2 (1-8 workers)
*   **Optimization:** Auto-terminates after 30 minutes.

---

## 📥 4. Ingestion & Bronze Layer Schema

Raw .csv data is ingested into `bronze.raw_sensor_table` (columns: name, sensor_id, timestamp, temperature, pressure).

---

## 🧹 5. Data Cleansing & Validation Layer (Silver Layer)

Uses Spark SQL and Delta Lake constraints to validate data and create `gold_clean_validated` (event_id, user_id, event_time, event_category).

---

## 📊 6. Gold Layer Dimensional Serving Logic

Builds star schemas (`gold_dim_customer`, `gold_fact_sales`) optimized for Power BI.

---

