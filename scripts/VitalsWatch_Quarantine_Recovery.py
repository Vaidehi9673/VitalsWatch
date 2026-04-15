import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import input_file_name, col, when, lit

# 1. Initialize Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_resource = boto3.resource('s3')

# Update with your specific bucket name
BUCKET_NAME = "vitalswatch-lakehouse-vaidehi"

# 2. READ: Load all quarantine CSVs
# We use input_file_name to determine which table the data belongs to
print(f"--- Starting VitalsWatch Recovery from s3://{BUCKET_NAME}/quarantine/ ---")

try:
    # Reading all CSVs in the quarantine folder
    raw_df = spark.read.csv(f"s3://{BUCKET_NAME}/quarantine/*.csv", header=True) \
                  .withColumn("source_file", input_file_name())
    
    row_count = raw_df.count()
except Exception as e:
    print(f"No files found in quarantine or error reading: {e}")
    row_count = 0

if row_count > 0:
    print(f"Total rows found in Quarantine: {row_count}")

    # The three tables we expect in your dataset
    target_tables = ["patients", "observations", "encounters"]

    for table in target_tables:
        # 3. FILTER: Isolate data for the current table using the filename
        table_df = raw_df.filter(col("source_file").contains(table))
        
        current_table_count = table_df.count()
        
        if current_table_count > 0:
            print(f">>> Processing {current_table_count} rows for Table: {table}")
            
            # 4. TRANSFORM: Schema-Aware Cleaning
            # We only fix the 'VALUE' column if it exists (typically in observations)
            if "VALUE" in table_df.columns:
                clean_df = table_df.withColumn(
                    "VALUE", 
                    when(col("VALUE").isNull(), lit("0"))
                    .when(col("VALUE") == "-999", lit("0"))
                    .otherwise(col("VALUE"))
                )
            else:
                # For Patients/Encounters, just fill basic nulls
                clean_df = table_df.fillna("Unknown")

            # Drop the internal helper column before saving
            final_df = clean_df.drop("source_file")

            # 5. WRITE: Append to the specific Silver sub-folder in Parquet format
            output_path = f"s3://{BUCKET_NAME}/silver/{table}/recovered/"
            
            final_df.write \
                .mode("append") \
                .parquet(output_path)
            
            print(f"Success: {table} data appended to {output_path}")

    # 6. CLEANUP: Delete Quarantine files only AFTER successful processing
    print("🧹 Cleaning up Quarantine folder to avoid duplicate processing...")
    bucket = s3_resource.Bucket(BUCKET_NAME)
    bucket.objects.filter(Prefix="quarantine/").delete()
    print("✨ Quarantine folder cleared.")

else:
    print("Quarantine folder is currently empty. Nothing to process.")

job.commit()
print("--- Job Run Finished ---")