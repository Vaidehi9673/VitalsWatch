import boto3
import pandas as pd
import io
import time
import random
import os
from dotenv import load_dotenv

# 1. Load credentials from .env
load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = "us-east-1" 

# 2. Configuration
S3_BUCKET = "vitalswatch-lakehouse-vaidehi" 

# check source file path 
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
LOCAL_DATA_PATH = os.path.join(ROOT_DIR, "source_files")
#print(f"Current Script Directory: {CURRENT_DIR}")
#print(f"Looking for data in: {LOCAL_DATA_PATH}")
# Verify the folder actually exists to avoid the 'File Not Found' error
if not os.path.exists(LOCAL_DATA_PATH):
    print("❌ ERROR: Still can't find the source_files folder. Please check the folder name.")

FILES_TO_PROCESS = ["patients.csv", "observations.csv", "encounters.csv"]

# 3. Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def introduce_errors(df, file_name):
    
    # Create a copy so we don't modify the original local data
    df_messy = df.copy()

    # FIX: Convert the VALUE column to 'object' type so it can accept 
    # both strings and integers without crashing.
    if 'VALUE' in df_messy.columns:
        df_messy['VALUE'] = df_messy['VALUE'].astype(object)
    
    # Strategy 1: Inject NULLs into 5% of the data
    for col in df_messy.columns:
        if random.random() < 0.05:
            df_messy.loc[df_messy.sample(frac=0.05).index, col] = None

    # Strategy 2: Domain-specific errors for Healthcare
    if file_name == "observations.csv":
        # Introduce negative values for 'VALUE' (e.g., Heart Rate or BP)
        error_indices = df_messy.sample(frac=0.02).index
        df_messy.loc[error_indices, 'VALUE'] = -999
        
    if file_name == "patients.csv":
        # Randomly remove some Patient IDs to test referential integrity
        error_indices = df_messy.sample(frac=0.01).index
        df_messy.loc[error_indices, 'Id'] = None

    # Strategy 3: Schema Drift / Data Type Error
    if random.random() < 0.03:
        # Putting a string in a numeric column to break downstream SQL/Spark
        df_messy.loc[df_messy.sample(n=1).index, 'VALUE'] = "MISSING_DATA"

    # Strategy 4: Logical Date Error
    if 'BIRTHDATE' in df_messy.columns:
        # Set some birthdates to the future
        df_messy.loc[df_messy.sample(frac=0.01).index, 'BIRTHDATE'] = "2099-12-31"

    return df_messy

def upload_to_s3():
    try:
        for file_name in FILES_TO_PROCESS:
            full_path = os.path.join(LOCAL_DATA_PATH, file_name)
            
            if not os.path.exists(full_path):
                print(f"Skipping {file_name}: File not found in {LOCAL_DATA_PATH}")
                continue

            print(f"Reading {file_name}...")
            df = pd.read_csv(full_path)
            
            # Make it messy
            messy_df = introduce_errors(df, file_name)
            
            # Convert to CSV in memory
            csv_buffer = io.StringIO()
            messy_df.to_csv(csv_buffer, index=False)
            
            # Create a unique filename using timestamp
            timestamp = int(time.time())
            target_key = f"bronze/{timestamp}_{file_name}"
            
            print(f"Uploading to s3://{S3_BUCKET}/{target_key}...")
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=target_key,
                Body=csv_buffer.getvalue()
            )
            print(f"Successfully uploaded {file_name}")

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    upload_to_s3()