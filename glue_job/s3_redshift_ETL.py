#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('test').getOrCreate()
glueContext = GlueContext(spark)
job = Job(glueContext)

source_data = 's3://poctest2025/healthcare_dataset.csv'

raw_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_data)

for column in raw_df.columns:
    if dict(raw_df.dtypes)[column] == 'string':
        raw_df = raw_df.withColumn(column, initcap(col(column)))

transformed_df = raw_df.withColumn('Blood_Type', upper(col('Blood_Type')))

transformed_df2 = transformed_df.withColumn('Billing_Amount', round(col('Billing_Amount'),1))

dyf = DynamicFrame.fromDF(transformed_df2, glueContext, "dyf")

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dyf,
    catalog_connection = 'Redshift_connection',
    connection_options = {
        "dbtable": "public.healthcare",
        "database": "dev"
        },
    redshift_tmp_dir = 's3://poctest2025/temp/'
)

