# TfL Real-Time Reliability & Data Engineering Platform
https://github.com/YashodaBojja/tfl-reliability-platform.git

## Project Overview
This project builds a production-style, serverless data engineering and reliability platform using AWS and Databricks.

The platform collects real-time arrival data for Transport for London bus line 244, stores the raw JSON data in Amazon S3, monitors operational health using Amazon CloudWatch, sends alerts using Amazon SNS, and transforms the data into Silver and Gold Delta tables in Databricks.

The infrastructure is deployed automatically using AWS CloudFormation and GitHub Actions. This implementation covers the ingestion, observability, CI/CD, drift detection, and Databricks transformation requirements of the capstone project.

## Architecture
GitHub → GitHub Actions → CloudFormation → AWS
TfL API → Lambda → S3 Bronze
S3 Bronze → Databricks Auto Loader → Silver → Gold
CloudWatch Metrics → Alarm → SNS Email

## AWS Resources
The main CloudFormation stack creates:
S3 Bronze bucket
Lambda function
IAM role
EventBridge five-minute schedule
CloudWatch metrics, alarm, and dashboard
SNS topic and email subscription

## CI/CD
The workflow file is:
.github/workflows/deploy.yml

Every push to the main branch validates and deploys template.yaml through GitHub Actions.

Required GitHub secrets:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
ALERT_EMAIL

## Data Pipeline
The Lambda function calls:
https://api.tfl.gov.uk/Line/244/Arrivals

Raw data is stored in:
s3://tfl-bronze-933832340858-eu-west-2-dev/raw/line-244/

The Lambda runs every five minutes.

## Monitoring
Custom CloudWatch metrics:
API_Success
Arrival_Count
Delay_Minutes
Maximum_Delay_Minutes
A CloudWatch alarm sends an SNS email when the maximum predicted wait exceeds 15 minutes.

## Databricks
Silver table:
workspace.tfl_reliability.silver_arrivals
Gold table:
workspace.tfl_reliability.gold_average_daily_delay_by_station

The Silver layer cleans and validates the raw data.
The Gold layer calculates average daily predicted wait by station.

## Drift Detection
Lambda memory was manually changed from 128 MB to 256 MB.
CloudFormation detected the Lambda resource as MODIFIED, and the configuration was restored through CloudFormation deployment.

## Evidence
Screenshots are available in the screenshots folder, including:
CloudWatch dashboard
GitHub Actions success
CloudFormation drift detection
Databricks Silver table
Databricks Gold table

## Future Improvements
GitHub OIDC instead of long-lived AWS keys
Lambda retries and dead-letter queue
Data freshness alarms
More TfL routes
Automated Databricks jobs