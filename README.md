# 🏥 VitalsWatch: End-to-End AWS Healthcare Data Lakehouse

## Project Overview
VitalsWatch is a fully automated, cloud-native Data Engineering pipeline built on Amazon Web Services (AWS). It processes, cleans, and analyzes synthetic patient healthcare records (demographics, clinical observations, and hospital encounters). 

The project transforms raw streaming data into a highly performant "Gold" layer (Lakehouse Architecture) to enable real-time Business Intelligence and healthcare risk profiling.

## AWS Cloud Architecture & Services Used
This project heavily utilizes the AWS ecosystem to build a scalable, serverless data pipeline:

* **Amazon S3 (Simple Storage Service):** Acted as the foundational Data Lake. Structured into a Medallion Architecture (`Bronze` for raw data, `Silver` for cleaned Parquet files, and `Gold` for business-ready aggregated tables).
* **AWS Lambda:** Serverless compute used for the ingestion layer. Triggered by incoming data payloads to instantly land raw records into the Bronze S3 bucket.
* **AWS Glue (Apache Spark):** The core ETL engine. Used PySpark distributed processing to clean data, enforce schemas, drop duplicates, and perform complex multi-table joins.
* **AWS Glue Data Catalog & Crawlers:** Automated metadata management. Crawlers scanned the S3 buckets to infer schemas and automatically create and update table definitions in the Data Catalog.
* **Amazon Athena:** Serverless interactive query service. Used Presto SQL to perform data profiling, validate ETL outputs, and execute complex business logic (e.g., segmenting patients into Health Risk tiers) directly against data in S3.
* **AWS IAM (Identity and Access Management):** Configured strict, least-privilege execution roles allowing Lambda, Glue, and Athena to securely communicate with specific S3 buckets.

## Additional Tech Stack
* **Languages:** Python (Generators/Boto3), PySpark, SQL
* **Data Formats:** JSON, CSV, Parquet
* **Business Intelligence:** Tableau Public

## Pipeline Flow
1. **Ingestion (`/ingestion`):** Python producers generate raw patient and encounter data. AWS Lambda captures the payload and lands it in S3 (Bronze).
2. **Transformation (`/scripts`):** PySpark jobs dynamically read from S3, handle schema variations, convert formats to columnar Parquet, and write to the Silver layer.
3. **Aggregation (`/scripts`):** A final PySpark job joins the `Patients`, `Observations`, and `Encounters` tables, creating a wide, denormalized summary table for BI tools (Gold).
4. **Analytics (`/sql`):** Amazon Athena queries the Gold layer using advanced SQL (CASE WHEN segmentations) to extract actionable business insights.
5. **Visualization:** Tableau connects to the summarized data to track hospital utilization and patient health risks.

## The Final Dashboard

![Patient Health & Demographics Dashboard](images/Patient_Segmentation.png)

## Key Data Engineering Challenges Overcome
* **Schema Evolution:** Used PySpark's `unionByName` with `allowMissingColumns=True` to seamlessly merge diverse, evolving data schemas in the Silver layer without breaking the pipeline.
* **Data Catalog Resilience:** Built a resilient "Direct-to-S3" read strategy in PySpark to bypass AWS Glue Crawler naming inconsistencies and ensure reliable downstream processing.
* **Data Skew Analysis:** Utilized Athena data profiling to identify highly right-skewed hospital encounter distributions. Pivoted the analytical approach to perform Health Risk (Vitals) segmentation, providing a much more accurate and visually compelling demographic report.