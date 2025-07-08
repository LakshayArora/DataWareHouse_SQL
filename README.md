# SQL Data Warehouse Project README

## Overview

This project demonstrates the creation and loading of a multi-layered data warehouse using T-SQL. It consists of a Bronze, Silver, and Gold architecture, handling data ingestion, cleansing, transformation, and analytics.

---

## Structure

### 1. **Database & Schemas**

* **Database**: `DataWareHouse` is dropped if it exists and recreated.
* **Schemas**:

  * `bronze`: Raw data ingestion layer.
  * `silver`: Cleaned and transformed data.
  * `gold`: Final analytics-ready views.

---

### 2. **Bronze Layer**

Raw data tables are created here, representing:

* CRM: Customer, Product, and Sales data
* ERP: Customer attributes, location, and product categorization

**Procedure**: `bronze.load_bronze`

* Truncates all tables
* Loads `.csv` files using `BULK INSERT`
* Applies minimal transformation (only schema mapping)

---

### 3. **Silver Layer**

This layer transforms and standardizes the Bronze data.

* Fixes inconsistent values (gender, marital status, etc.)
* Converts raw dates to valid formats
* Performs deduplication using `ROW_NUMBER()`
* Derives new fields like `prd_end_dt`

**Procedure**: `silver.load_silver`

* Truncates Silver tables
* Inserts transformed data from Bronze

---

### 4. **Gold Layer**

Gold layer provides analytical views:

* `dim_customers`: Enriched customer dimension
* `dim_products`: Cleaned product dimension
* `fact_sales`: Sales fact table joined with customer/product dimensions

---

### 5. **Analytics & Insights**

Analytical queries are provided for:

* Sales summaries
* Category-wise revenue
* Customer segmentation (VIP, Regular, New)
* Product cost brackets
* Revenue trend over time
* Performance benchmarking (current vs avg vs previous year)

---

### 6. **Customer Report View**

* `gold.customer_report` provides customer-level metrics:

  * Orders placed
  * Total/average sales
  * Product count
  * Order recency
  * Segment classification

---

## Data Sources

All data files are stored locally and ingested using paths like:

```
D:\SQLBaraa\sql-data-warehouse-project\datasets\source_crm\...
```

Update paths as per your local environment.

---

## Execution Order

1. Run **database/schema creation** script
2. Run **Bronze layer creation** script
3. Execute `EXEC bronze.load_bronze`
4. Run **Silver layer creation** script
5. Execute `EXEC silver.load_silver`
6. Run **Gold layer views and analytics**

---

## Requirements

* SQL Server (tested on SSMS)
* CSV files in proper path

---

## Contact

Author: Lakshay Arora
LinkedIn: [lakshay-arora-881042136](https://www.linkedin.com/in/lakshay-arora-881042136)
