import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# 1. Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.caseSensitive", "false")

# CONFIGURATION
BUCKET = "vitalswatch-lakehouse-vaidehi"
GOLD_PATH = f"s3://{BUCKET}/gold/patient_360_summary/"

print("--- Starting Gold Join using Direct S3 Reads ---")

# 2. THE HELPER FUNCTION
def load_and_unify_from_s3(entity_name):
    csv_path = f"s3://{BUCKET}/silver/{entity_name}/original_clean/"
    parquet_path = f"s3://{BUCKET}/silver/{entity_name}/recovered/"
    
    df_csv = None
    df_parquet = None
    
    try:
        df_csv = spark.read.option("header", "true").csv(csv_path)
    except Exception:
        pass

    try:
        df_parquet = spark.read.parquet(parquet_path)
    except Exception:
        pass
        
    if df_csv is not None and df_parquet is not None:
        return df_csv.unionByName(df_parquet, allowMissingColumns=True)
    elif df_csv is not None:
        return df_csv
    elif df_parquet is not None:
        return df_parquet
    else:
        raise ValueError(f"CRITICAL: No data found for {entity_name}!")

# 3. LOAD DATA 
try:
    patients_df = load_and_unify_from_s3("patients")
    obs_df = load_and_unify_from_s3("observations")
    encounters_df = load_and_unify_from_s3("encounters")

    # 4. TRANSFORM & AGGREGATE
    obs_agg = obs_df.groupBy("PATIENT").agg(
        F.round(F.avg(F.col("VALUE").cast("double")), 2).alias("avg_vital_value"),
        F.count("*").alias("total_observations") 
    )
    
    enc_agg = encounters_df.groupBy("PATIENT").agg(
        F.count("*").alias("total_encounters")    
    )

    # 5. THE MASTER JOIN
    # FIXED: Replaced "Id", "FIRST", "LAST", "GENDER" with the exact names from the logs
    gold_df = patients_df.join(obs_agg, patients_df["patient"] == obs_agg["PATIENT"], "left") \
        .join(enc_agg, patients_df["patient"] == enc_agg["PATIENT"], "left") \
        .select(
            patients_df["patient"].alias("patient_id"),
            patients_df["first"].alias("first_name"),
            patients_df["last"].alias("last_name"), 
            patients_df["gender"].alias("gender"), 
            "avg_vital_value", "total_observations", "total_encounters"
        )
        
    final_gold_df = gold_df.fillna({
        "avg_vital_value": 0.0, 
        "total_observations": 0, 
        "total_encounters": 0
    })

    # 6. WRITE TO GOLD
    final_gold_df.write.mode("overwrite").parquet(GOLD_PATH)
    print(f" Success! Gold Patient 360 data created at: {GOLD_PATH}")

except Exception as e:
    print(f"Pipeline Failed: {str(e)}")

job.commit()