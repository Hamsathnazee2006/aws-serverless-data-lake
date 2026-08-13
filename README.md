# AWS Serverless Data Lake & ETL Analytics Platform

An end-to-end AWS data engineering project that ingests raw CSV data into Amazon S3, catalogs it with AWS Glue, transforms it using Glue ETL/PySpark, stores analytics-ready data as Apache Parquet, and queries the processed data using Amazon Athena.

## Project Overview

This project implements an e-commerce data lake containing customers, products, and orders.

### Objectives

- Store raw data in Amazon S3
- Discover schemas using AWS Glue Crawlers
- Maintain metadata in the Glue Data Catalog
- Transform and join datasets using AWS Glue ETL
- Calculate revenue metrics
- Store processed data as Parquet
- Catalog processed datasets
- Analyze data using Athena SQL

## Architecture

```text
Raw CSV
   |
   v
Amazon S3 - Raw Layer
   |
   v
AWS Glue Crawler
   |
   v
Glue Data Catalog
   |
   v
AWS Glue ETL / PySpark
   |
   v
Amazon S3 - Processed Layer
   |
   v
Apache Parquet
   |
   v
Processed Glue Crawler
   |
   v
Glue Data Catalog
   |
   v
Amazon Athena
   |
   v
SQL Analytics
```

## AWS Services

| Service | Purpose |
|---|---|
| Amazon S3 | Raw and processed data storage |
| AWS Glue Crawler | Schema discovery and catalog creation |
| AWS Glue Data Catalog | Table and schema metadata |
| AWS Glue ETL | Transformation using PySpark |
| Apache Parquet | Columnar analytics storage |
| Amazon Athena | Serverless SQL analytics |
| IAM | Access control |
| CloudWatch | Job logs and monitoring |

## S3 Data Lake Structure

```text
s3://<your-bucket>/
├── raw/
│   ├── customers/
│   │   └── customers.csv
│   ├── orders/
│   │   └── orders.csv
│   └── products/
│       └── products.csv
└── processed/
    ├── customer_orders/
    │   └── *.parquet
    ├── product_sales/
    │   └── *.parquet
    └── category_sales/
        └── *.parquet
```

## Source Data

### Customers

| Column | Description |
|---|---|
| customer_id | Unique customer ID |
| customer_name | Customer name |
| email | Customer email |
| city | Customer city |
| state | Customer state |

### Products

| Column | Description |
|---|---|
| product_id | Unique product ID |
| product_name | Product name |
| category | Product category |
| price | Product price |

### Orders

| Column | Description |
|---|---|
| order_id | Unique order ID |
| customer_id | Customer ID |
| order_date | Order date |
| product_id | Product ID |
| quantity | Quantity purchased |

## Complete Implementation

### 1. Create S3

Create a bucket in your selected AWS Region.

Example:

```text
hamsath-aws-data-lake-2026
```

Create:

```text
raw/
processed/
```

Inside `raw/`:

```text
raw/customers/
raw/orders/
raw/products/
```

Upload the corresponding CSV files.

### 2. Create Glue Database

Open:

```text
AWS Glue
→ Data Catalog
→ Databases
→ Add database
```

Create:

```text
aws_datalake_db
```

### 3. Create Raw Crawler

Create an S3 Glue crawler targeting:

```text
s3://<your-bucket>/raw/
```

Use database:

```text
aws_datalake_db
```

Run it.

Expected tables:

```text
customers
orders
products
```

### 4. Verify Raw Tables

Open:

```text
AWS Glue
→ Data Catalog
→ Tables
```

Verify the schemas.

If fields appear as `col0`, `col1`, `col2`, check the CSV header, data format, crawler settings, and S3 path.

## Glue ETL

The ETL job reads:

```text
customers
orders
products
```

and produces:

```text
customer_orders
product_sales
category_sales
```

### Join logic

```text
orders.customer_id = customers.customer_id
orders.product_id = products.product_id
```

### Revenue calculation

```text
revenue = quantity × price
```

### Avoiding ambiguous columns

Use DataFrame aliases and qualified columns when joined datasets contain duplicate column names:

```python
orders.alias("o")
customers.alias("c")
products.alias("p")
```

Then reference columns explicitly:

```python
col("o.customer_id")
col("c.customer_name")
col("p.product_name")
```

This prevents `AMBIGUOUS_REFERENCE` errors.

## Processed Datasets

### customer_orders

Typical fields:

```text
order_id
customer_id
customer_name
email
city
state
order_date
product_id
product_name
category
quantity
price
revenue
```

### product_sales

Typical metrics:

```text
product_id
product_name
total_quantity
total_revenue
```

### category_sales

Typical metrics:

```text
category
total_quantity
total_revenue
total_orders
```

## Parquet Output

The ETL job writes to:

```text
s3://<your-bucket>/processed/
```

Expected folders:

```text
processed/customer_orders/
processed/product_sales/
processed/category_sales/
```

Objects should be similar to:

```text
part-00000-xxxx.snappy.parquet
```

Parquet is a columnar format suited to analytical workloads and can reduce unnecessary data scanning compared with raw CSV.

## Verify the Glue Job

Open:

```text
AWS Glue
→ ETL jobs
→ Your job
→ Runs
```

Expected:

```text
Succeeded
```

For failures, inspect run details and CloudWatch logs.

Common issues:

- Incorrect S3 path
- IAM permission errors
- Incorrect table name
- Schema mismatch
- Ambiguous columns
- Empty source data

## Processed Data Crawler

Create a second Glue crawler targeting:

```text
s3://<your-bucket>/processed/
```

Use:

```text
aws_datalake_db
```

Run it.

Expected tables:

```text
customer_orders
product_sales
category_sales
```

## Athena

Open:

```text
Amazon Athena
→ Query Editor
```

Select:

```text
Data source: AwsDataCatalog
Database: aws_datalake_db
```

Test:

```sql
SELECT *
FROM customer_orders
LIMIT 10;
```

A successful result confirms Athena can read the processed Parquet data.

## Athena SQL Analytics

### Overall KPIs

```sql
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(revenue) AS total_revenue
FROM customer_orders;
```

### Top Products

```sql
SELECT
    product_id,
    product_name,
    SUM(quantity) AS total_quantity,
    SUM(revenue) AS total_revenue
FROM customer_orders
GROUP BY product_id, product_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### Category Performance

```sql
SELECT
    category,
    total_quantity,
    total_revenue,
    total_orders
FROM category_sales
ORDER BY total_revenue DESC;
```

### Top Customers

```sql
SELECT
    customer_id,
    customer_name,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue) AS total_spent
FROM customer_orders
GROUP BY customer_id, customer_name
ORDER BY total_spent DESC
LIMIT 10;
```

### Daily Revenue

```sql
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue) AS total_revenue
FROM customer_orders
GROUP BY order_date
ORDER BY order_date;
```

## Save Athena Queries

Recommended names:

```text
01_total_business_kpis
02_top_products
03_category_analysis
04_top_customers
05_daily_revenue
```

Keep the SQL files in the repository.

## GitHub Repository Structure

```text
aws-data-lake-project/
├── README.md
├── data/
│   └── raw/
│       ├── customers.csv
│       ├── orders.csv
│       └── products.csv
├── glue/
│   └── aws-data-lake-etl.py
├── athena/
│   ├── 01_total_business_kpis.sql
│   ├── 02_top_products.sql
│   ├── 03_category_analysis.sql
│   ├── 04_top_customers.sql
│   └── 05_daily_revenue.sql
└── screenshots/
    ├── 01-s3-raw-data.png
    ├── 02-glue-database.png
    ├── 03-glue-raw-tables.png
    ├── 04-glue-etl-script.png
    ├── 05-glue-job-success.png
    ├── 06-s3-processed-parquet.png
    ├── 07-glue-processed-tables.png
    ├── 08-athena-customer-orders.png
    ├── 09-athena-category-analysis.png
    └── 10-athena-business-kpis.png
```

## Validation Checklist

- [ ] S3 bucket created
- [ ] Raw folders created
- [ ] CSV files uploaded
- [ ] Glue database created
- [ ] Raw crawler succeeded
- [ ] Customers, orders and products tables created
- [ ] Glue ETL job succeeded
- [ ] Processed Parquet files created
- [ ] Processed crawler succeeded
- [ ] Customer orders, product sales and category sales tables created
- [ ] Athena query succeeded
- [ ] KPI query tested
- [ ] Product query tested
- [ ] Category query tested
- [ ] Customer query tested
- [ ] Daily revenue query tested
- [ ] SQL files added to GitHub
- [ ] Glue script added to GitHub
- [ ] Screenshots added

## Troubleshooting

### Athena: TABLE_NOT_FOUND

Check:

```text
AWS Region
Database name
Table name
Glue Data Catalog
```

Refresh Athena after the crawler finishes.

### Glue: AMBIGUOUS_REFERENCE

Use aliases and qualified columns:

```python
orders.alias("o")
customers.alias("c")
products.alias("p")
```

For example:

```python
col("o.customer_id")
col("c.customer_name")
col("p.product_name")
```

### Glue table contains col0, col1, col2

Check:

- CSV header
- CSV format
- Crawler configuration
- S3 source path

### Processed S3 folder is empty

Check:

1. Glue job status
2. Glue logs
3. Output S3 path
4. IAM permissions
5. Source table row counts
6. Transformation output

### Athena returns zero rows

Run:

```sql
SELECT COUNT(*)
FROM customer_orders;
```

If zero, inspect the processed S3 output and Glue transformation.

## Security

Never commit:

```text
AWS access keys
AWS secret keys
Session tokens
Private keys
Passwords
Credential files
.env files containing secrets
```

Use IAM roles for AWS services whenever possible.

Use synthetic or non-sensitive data in a public repository.

## Cost Management

AWS services can incur charges depending on usage.

For a learning project:

- Keep datasets small.
- Avoid unnecessary repeated Glue runs.
- Avoid scanning unnecessary data in Athena.
- Delete unused S3 objects.
- Remove unused AWS resources after testing.
- Monitor AWS billing.

## Recommended Screenshots

Capture:

```text
01-s3-raw-data.png
02-glue-database.png
03-glue-raw-tables.png
04-glue-etl-script.png
05-glue-job-success.png
06-s3-processed-parquet.png
07-glue-processed-tables.png
08-athena-customer-orders.png
09-athena-category-analysis.png
10-athena-business-kpis.png
```

Do not include credentials or sensitive information in screenshots.

## Future Improvements

Possible enhancements:

- AWS Lambda event-driven ingestion
- AWS Step Functions orchestration
- EventBridge scheduling
- Glue Data Quality checks
- Date-based partitioning
- S3 lifecycle policies
- CloudWatch alarms
- Incremental ETL
- Schema evolution handling
- GitHub Actions CI/CD
- Amazon QuickSight dashboards
- Stronger data validation
- Encryption and least-privilege IAM policies

## Final Architecture Summary

```text
CSV
 ↓
Amazon S3 Raw
 ↓
AWS Glue Crawler
 ↓
Glue Data Catalog
 ↓
AWS Glue ETL / PySpark
 ↓
Amazon S3 Processed
 ↓
Apache Parquet
 ↓
Processed Glue Crawler
 ↓
Glue Data Catalog
 ↓
Amazon Athena
 ↓
SQL Analytics
```

## Project Status

**End-to-end AWS data lake pipeline implemented:**

```text
Ingest → Catalog → Transform → Store → Catalog → Query → Analyze
```

## Author

**A. Mohamed Hamsath Nazeer**

B.Tech Information Technology
