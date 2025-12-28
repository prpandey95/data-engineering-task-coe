## Overview
This project ingests raw CSV files into Delta Lake
Databricks SQL and further trasnform it for analytics purpose.

# Prerequisites
- Databricks workspace (Free Edition is sufficient)
- Access to Databricks SQL Warehouse
- Unity Catalog enabled (default in Free Edition)

# Folder Structure

├── sql/
│   ├── 01_bronze.sql
│   ├── 02_silver.sql
│   ├── 03_gold.sql
│   ├── 04_analysis_answers.sql
│   └── 05_optional.sql
├── data/
│   ├── customers.csv
│   ├── subscriptions.csv
│   ├── payments.csv
│   └── payments_incremental.csv
└── README.md

## Bronze Layer

### 1. Upload Data
1. In Databricks, go to **Catalog**
2. Navigate to workspace -> default
3. Click on the Create Dropdown on the top right and select Volume. This is to upload and keep the data files in dbfs (Databricks File System).
4. Create a Volume named `raw_csv`
5. Volume Type -> Managed Volume, Select a catalog -> workspace, Select a schema -> default
6. Once the volume is created it will appear inside default schema (workspace -> default -> raw_csv)
7. Click the newly created volume.
8. Click on 'Upload to this Volume' on the top right and select the data files to upload (customers.csv, subscriptions.csv, payments.csv)

Files should be available at:
/Volumes/main/default/raw_csv/

### 2. Start SQL Warehouse
- Go to **SQL → SQL Warehouses**
- Start the default warehouse if not already started

### 3. Run SQL Script
- Open **SQL Editor**
- Click on 'SQL Query' below Create New to create and open new SQL Query Editor Tab
- Execute `sql/01_bronze.sql`
-Attach the Serverless Starter Warehouse if prompted.
- A new schema 'bronze' will be created inside catalog -> workspace where the tables will reside

### Tables Created
- bronze.customers
- bronze.subscriptions
- bronze.payments

Each table includes:
- All raw fields from CSV
- `ingested_at` (timestamp)
- `source_file` (string)

## Silver Layer
- Execute `sql/02_silver.sql` (Create views using the bronze tables)

### Views Created
- silver.customers (converted signup_date column from bronze.customers to date data type)
- silver.subscriptions_dedup (converted start_date and end_date column from bronze.subscriptions to date data type. Removed records with duplicate subscription_id.)
- silver.payments_success_dedup (converted paid_at column from bronze.payments to datetime data type. Removed records with duplicate payment_id and included only the records with payment_status = 'SUCCESS')
- silver.subscriptions_active_daily (Used mix of functions like EXPLODE, SEQUENCE, GREATEST and LEAST to prepare the data of members that were active in the last 90 days including current date)

## Gold Layer
- Execute `sql/03_gold.sql` (Create views using the silver tables)

### Views Created
- gold.mrr_by_month_plan
- gold.churn_rate_by_month
- gold.top_10_countries_by_revenue

### Rules/Assumptions
#### gold.mrr_by_month_plan
- Aggregate payments per subscription by month (based on paid_at)
- Sum amounts per plan to compute monthly MRR

##### Currency Handling:
- Amounts are converted to USD:
- USD → as-is
- PHP → converted using a fixed rate (1 USD = 58.57 PHP)

#### gold.churn_rate_by_month
- A customer is considered churned in month M if their subscription end_date falls within month M
- Active customers at the start of the month are counted as the denominator
- Churn rate is calculated as: churn_rate = (# of churned customers in month M) / (# of active customers at start of month M)

## Analysis Answers
1. PLUS plan has the highest growth over the last 3 months but this is in negative as we are seeing decrement in MRR for all plans for recent month compared to 2 months ago. And this plan had the least decrement.
2. We are seeing decrement in churn rate. 2025-10 had 16 churned customers, 2025-11 saw that number drop down to 11 and for 2025-12 there are 0 churns.
Country PH has contributed most to the churn with a whopping count of 65.
3. As mentioned in the task itself we had issue regarding duplicate data. Other than that we also had NULL in end_date which can be valid based on the duration of the plan chosen by the customer. Lastly we also had issue regarding uniformity in data for column Currency in payments.csv with USD and PHP both present.
