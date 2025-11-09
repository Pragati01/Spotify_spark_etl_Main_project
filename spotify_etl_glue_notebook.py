import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# 1️⃣ Initialize Glue job
args = getResolvedOptions(sys.argv, ['Spotify_ETL'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['Spotify_ETL'], args)

# 2️⃣ Read data from S3 (or from Glue Catalog)
input_path = "s3://my-bucket/raw/customer_events/"
df = spark.read.json(input_path)

# 3️⃣ Transform data
df_cleaned = (
    df.filter(df.user_id.isNotNull())
      .withColumn("event_date", F.to_date("timestamp"))
)

df_summary = (
    df_cleaned.groupBy("user_id", "event_date")
              .agg(F.count("*").alias("event_count"))
)

# 4️⃣ Write back to S3 in Parquet format
output_path = "s3://my-bucket/processed/customer_events_summary/"
df_summary.write.mode("overwrite").parquet(output_path)

# 5️⃣ Commit job so Glue knows it finished successfully
job.commit()
