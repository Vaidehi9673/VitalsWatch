import boto3
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO

# 1. Load environment variables from your .env file
load_dotenv()

# 2. Access variables
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# 3. Initialize S3 Client using the .env credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# Path Setup
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
LOCAL_DATA_PATH = os.path.join(ROOT_DIR, "source_files")

def upload_clean_data():
    # Verify bucket name is loaded
    if not BUCKET_NAME:
        print("❌ Error: AWS_BUCKET_NAME not found in .env file.")
        return

    files = ["patients.csv", "observations.csv", "encounters.csv"]
    timestamp = int(datetime.now().timestamp())
    
    for file_name in files:
        full_path = os.path.join(LOCAL_DATA_PATH, file_name)
        
        if os.path.exists(full_path):
            print(f"Reading {file_name}...")
            df = pd.read_csv(full_path)
            
            # Take a clean sample
            clean_df = df.sample(n=500) 
            
            # Convert to CSV buffer
            csv_buffer = StringIO()
            clean_df.to_csv(csv_buffer, index=False)
            
            # Define the Bronze target key
            target_key = f"bronze/CLEAN_BATCH_{timestamp}_{file_name}"
            
            # Upload to S3
            try:
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=target_key,
                    Body=csv_buffer.getvalue()
                )
                print(f"✅ Successfully uploaded {file_name} to Bronze.")
            except Exception as e:
                print(f"❌ Failed to upload {file_name}: {str(e)}")
        else:
            print(f"⚠️ Warning: {file_name} not found in {LOCAL_DATA_PATH}")

if __name__ == "__main__":
    upload_clean_data()