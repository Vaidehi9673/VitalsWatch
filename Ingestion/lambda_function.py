import boto3
import pandas as pd
import io

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("--- VitalsWatch: Row-Level Routing Started ---")
    
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        file_name = key.split('/')[-1]
        
        # 1. Load Data
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(response['Body'])
        total_rows = len(df)
        
        # 2. Identify "Bad" rows (those with any Nulls)
        # You can add more conditions here (like VALUE == -999)
        is_bad = df.isnull().any(axis=1)
        
        # 3. Split the DataFrame
        bad_df = df[is_bad]
        clean_df = df[~is_bad]
        
        print(f"Total: {total_rows} | Clean: {len(clean_df)} | Quarantined: {len(bad_df)}")

        # 4. Save Clean Rows to Silver
        if not clean_df.empty:
            silver_key = f"silver/CLEAN_{file_name}"
            csv_buffer = io.StringIO()
            clean_df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=silver_key, Body=csv_buffer.getvalue())
            print(f"✅ Exported {len(clean_df)} rows to Silver.")

        # 5. Save Bad Rows to Quarantine
        if not bad_df.empty:
            quarantine_key = f"quarantine/DIRTY_{file_name}"
            csv_buffer = io.StringIO()
            bad_df.to_csv(csv_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=quarantine_key, Body=csv_buffer.getvalue())
            print(f"❌ Exported {len(bad_df)} rows to Quarantine.")

        # 6. Cleanup Bronze
        #s3.delete_object(Bucket=bucket, Key=key)
        
        return {'statusCode': 200, 'body': 'Routing Successful'}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}