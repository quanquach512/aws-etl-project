import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read products table
df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_db",
    table_name="products_api"
).toDF()

# Select + rename columns
df = df.select(
    col("id").alias("product_id"),
    col("title").alias("product_name"),
    col("category"),
    col("brand"),
    col("price").alias("catalog_price")
)

# Write to S3 as Parquet
df.write \
  .mode("overwrite") \
  .parquet("s3://quanquach-aws-etl-project-2026/processed/dim_products/")

job.commit()