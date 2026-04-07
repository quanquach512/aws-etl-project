import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_db",
    table_name="transactions"
).toDF()

# Transform
df = df.withColumn("total_amount", col("quantity") * col("unit_price"))
df = df.withColumn("year", year(col("transaction_date")))
df = df.withColumn("month", month(col("transaction_date")))

# Write to S3 as Parquet
df.write \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .parquet("s3://quanquach-aws-etl-project-2026/processed/fact_transactions/")

job.commit()