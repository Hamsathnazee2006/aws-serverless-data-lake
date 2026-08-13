import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ============================================================
# AWS GLUE DATA LAKE ETL - FULL CORRECTED VERSION
# Job: aws-data-lake-etl-2026
# Database: aws_datalake_db
# Input tables: customers, products, orders
# Output: S3 processed/ as Parquet
# ============================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------
# Project configuration
# -----------------------------
DATABASE = "aws_datalake_db"
BUCKET = "hamsath-aws-data-lake-2026"

CUSTOMER_ORDERS_PATH = f"s3://{BUCKET}/processed/customer_orders/"
PRODUCT_SALES_PATH = f"s3://{BUCKET}/processed/product_sales/"
CATEGORY_SALES_PATH = f"s3://{BUCKET}/processed/category_sales/"

print("========== AWS DATA LAKE ETL STARTED ==========")

# ============================================================
# 1. READ CUSTOMERS
# ============================================================

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="customers"
)
customers_df = customers_dyf.toDF()

customers_df = customers_df.select(
    F.col("customer_id").cast("string").alias("customer_id"),
    F.col("name").cast("string").alias("name"),
    F.col("email").cast("string").alias("email"),
    F.col("city").cast("string").alias("city"),
    F.col("state").cast("string").alias("state")
).dropDuplicates(["customer_id"])

print("Customers schema:")
customers_df.printSchema()

# ============================================================
# 2. READ PRODUCTS
# ============================================================

products_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="products"
)
products_df = products_dyf.toDF()

products_df = products_df.select(
    F.col("product_id").cast("string").alias("product_id"),
    F.col("product_name").cast("string").alias("product_name"),
    F.col("category").cast("string").alias("category"),
    F.col("price").cast("double").alias("price")
).dropDuplicates(["product_id"])

print("Products schema:")
products_df.printSchema()

# ============================================================
# 3. READ ORDERS
# ============================================================

orders_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name="orders"
)
orders_df = orders_dyf.toDF()

orders_df = orders_df.select(
    F.col("order_id").cast("string").alias("order_id"),
    F.col("customer_id").cast("string").alias("customer_id"),
    F.col("product_id").cast("string").alias("product_id"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("order_date").cast("string").alias("order_date")
).filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
)

print("Orders schema:")
orders_df.printSchema()

# ============================================================
# 4. JOIN ORDERS + PRODUCTS
# Explicitly select columns to prevent duplicate product_id
# ============================================================

o = orders_df.alias("o")
p = products_df.alias("p")

order_product_df = (
    o.join(
        p,
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("o.product_id").alias("product_id"),
        F.col("o.quantity").alias("quantity"),
        F.col("o.order_date").alias("order_date"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.category").alias("category"),
        F.col("p.price").alias("price")
    )
)

# ============================================================
# 5. CALCULATE REVENUE
# ============================================================

order_product_df = order_product_df.withColumn(
    "revenue",
    F.round(
        F.col("quantity").cast("double") * F.col("price").cast("double"),
        2
    )
)

print("Order + Product schema:")
order_product_df.printSchema()

# ============================================================
# 6. JOIN CUSTOMERS
# Explicitly qualify both customer_id columns
# ============================================================

op = order_product_df.alias("op")
c = customers_df.alias("c")

customer_orders_df = (
    op.join(
        c,
        F.col("op.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .select(
        F.col("op.order_id").alias("order_id"),
        F.col("op.customer_id").alias("customer_id"),
        F.col("c.name").alias("customer_name"),
        F.col("c.email").alias("email"),
        F.col("c.city").alias("city"),
        F.col("c.state").alias("state"),
        F.col("op.product_id").alias("product_id"),
        F.col("op.product_name").alias("product_name"),
        F.col("op.category").alias("category"),
        F.col("op.quantity").alias("quantity"),
        F.col("op.price").alias("price"),
        F.col("op.revenue").alias("revenue"),
        F.col("op.order_date").alias("order_date")
    )
    .filter(
        F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
    )
)

print("Final customer_orders schema:")
customer_orders_df.printSchema()

# ============================================================
# 7. PRODUCT SALES
# ============================================================

product_sales_df = (
    customer_orders_df
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders")
    )
)

# ============================================================
# 8. CATEGORY SALES
# ============================================================

category_sales_df = (
    customer_orders_df
    .groupBy("category")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders")
    )
)

# ============================================================
# 9. WRITE PROCESSED PARQUET DATA TO S3
# ============================================================

print("Writing customer_orders...")
customer_orders_df.write.mode("overwrite").format("parquet").save(
    CUSTOMER_ORDERS_PATH
)

print("Writing product_sales...")
product_sales_df.write.mode("overwrite").format("parquet").save(
    PRODUCT_SALES_PATH
)

print("Writing category_sales...")
category_sales_df.write.mode("overwrite").format("parquet").save(
    CATEGORY_SALES_PATH
)

# ============================================================
# 10. RESULT COUNTS
# ============================================================

print("========== ETL OUTPUT SUMMARY ==========")
print("Customer orders rows:", customer_orders_df.count())
print("Product sales rows:", product_sales_df.count())
print("Category sales rows:", category_sales_df.count())

print("========== ETL COMPLETED SUCCESSFULLY ==========")

job.commit()
