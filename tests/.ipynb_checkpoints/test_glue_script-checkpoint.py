from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, upper, round

spark = SparkSession.builder.appName('GlueTest').getOrCreate()

source_data = 's3://poctest2025/healthcare_dataset.csv'

raw_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(source_data)

for column in raw_df.columns:
    if dict(raw_df.dtypes)[column] == 'string':
        raw_df = raw_df.withColumn(column, initcap(col(column)))

transformed_df = raw_df.withColumn('Blood_Type', upper(col('Blood_Type')))
transformed_df2 = transformed_df.withColumn('Billing_Amount', round(col('Billing_Amount'), 2))

# ✅ Test Output
print("Schema:")
transformed_df2.printSchema()

print("Sample Data:")
transformed_df2.show(5)

# Check row count
row_count = transformed_df2.count()
if row_count == 0:
    raise Exception("Validation failed: No rows in transformed DataFrame")

# Check required columns exist
required_columns = ['Blood_Type', 'Billing_Amount']
missing = [col for col in required_columns if col not in transformed_df2.columns]
if missing:
    raise Exception(f"Validation failed: Missing columns {missing}")

print("✅ Validation passed. Proceeding to deploy.")
