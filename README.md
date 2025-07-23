# 🧱 Data Engineering Pipeline Project (PySpark + Delta Lake)

This project demonstrates an **end-to-end data engineering pipeline** built using **Apache Spark**, **Delta Lake**, and Python — without relying on Azure-specific infrastructure like Azure Databricks or ADLS Gen2. The goal is to simulate real-world batch data processing and analytics with clean modular architecture.

---

## 📌 Problem Statement

This pipeline handles transaction data and product data from multiple sales channels and processes it for downstream analytics.

### The main objectives are:

1. **Load Transaction Data**
   - Load CSV-based transaction data into Spark DataFrames from a local file system (simulating ADLS Gen2).

2. **Extract Insights**
   - Join transaction data with product information.
   - Derive key insights:
     - Average order value per customer.
     - Most popular products and categories.
     - Sales performance influenced by marketing campaigns.

3. **Create Managed Delta Table**
   - Store enriched insights in a **Delta Lake managed table**.

4. **Optimize Data Storage**
   - Use Delta Lake snapshotting and file compaction for efficient storage and querying.

5. **Monitor Data Quality**
   - Detect missing values, nulls, and outliers.
   - Handle data from **multiple sales channels** (web, mobile, in-store).

---

## 📁 Project Structure

```bash
data_pipeline_project/
│
├── data/
│   ├── transactions.csv       # Raw transaction data
│   └── products.csv           # Product master data
│
├── pipeline/
│   ├── __init__.py
│   ├── loader.py              # Load CSV files into Spark
│   ├── transformer.py         # Join and transform data
│   ├── validator.py           # Data quality checks
│   └── writer.py              # Write to Delta Lake
│
├── main.py                    # Entry script
├── requirements.txt           # Dependencies
└── README.md                  # Project overview (this file)




