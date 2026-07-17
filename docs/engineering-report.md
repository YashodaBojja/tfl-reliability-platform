# Engineering Report — TfL Reliability Platform

## Overview
This project collects real-time TfL line 244 arrival data using AWS Lambda. EventBridge runs the Lambda every five minutes, and the raw JSON data is stored in an Amazon S3 Bronze bucket.
CloudFormation manages the AWS infrastructure, and GitHub Actions automatically validates and deploys changes when code is pushed to the main branch.

## Reliability Strategy
The Lambda publishes custom CloudWatch metrics:
- `API_Success`
- `Arrival_Count`
- `Delay_Minutes`
- `Maximum_Delay_Minutes`

If the TfL API fails, Lambda records the error in CloudWatch Logs and publishes `API_Success = 0`.
A CloudWatch alarm sends an SNS email notification when the maximum predicted waiting time exceeds 15 minutes.
The CloudWatch dashboard helps identify API failures, missing data, and increased arrival waiting times.

## Data Processing
Raw JSON is stored in the Bronze layer.
Databricks Auto Loader reads the S3 files and creates a Silver Delta table. Records with null station names, null arrival times, or negative values are removed.
The Gold table calculates the average daily predicted waiting time for each station.

## CI/CD and Drift Detection
GitHub Actions automatically deploys the CloudFormation template.
Drift detection was tested by manually changing Lambda memory from 128 MB to 256 MB. CloudFormation detected the resource as modified, and the correct configuration was restored through deployment.

## Future Improvements
Future improvements include:
- Lambda retries
- SQS dead-letter queue
- Data freshness alarms
- GitHub OIDC instead of stored AWS keys
- Monitoring more TfL routes
- Automated Databricks jobs