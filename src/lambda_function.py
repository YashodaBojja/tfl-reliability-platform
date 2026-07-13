import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def fetch_tfl_arrivals(line_id: str) -> tuple[list, int]:
    """
    Fetch live arrival data from the TfL Unified API.
    """

    api_url = f"https://api.tfl.gov.uk/Line/{line_id}/Arrivals"

    request = urllib.request.Request(
        api_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "tfl-reliability-platform/1.0",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status_code = response.getcode()
            response_body = response.read().decode("utf-8")

            return json.loads(response_body), status_code

    except urllib.error.HTTPError as error:
        logger.exception(
            "TfL API returned HTTP error %s",
            error.code,
        )
        raise

    except urllib.error.URLError:
        logger.exception("Unable to connect to the TfL API")
        raise

    except json.JSONDecodeError:
        logger.exception("TfL API returned invalid JSON")
        raise


def save_to_s3(
    bucket_name: str,
    line_id: str,
    arrival_data: list,
    api_status_code: int,
) -> str:
    """
    Store the unmodified TfL API response in the S3 Bronze bucket.
    """

    current_time = datetime.now(timezone.utc)

    object_key = current_time.strftime(
        f"raw/line-{line_id}/year=%Y/month=%m/day=%d/hour=%H/"
        f"arrivals-%Y%m%dT%H%M%SZ.json"
    )

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json.dumps(arrival_data),
        ContentType="application/json",
        Metadata={
            "line-id": line_id,
            "api-status-code": str(api_status_code),
            "ingested-at": current_time.isoformat(),
        },
    )

    return object_key


def lambda_handler(event, context):
    bucket_name = os.environ["BRONZE_BUCKET_NAME"]
    line_id = os.environ.get("TFL_LINE_ID", "244")

    logger.info("Starting TfL ingestion for line %s", line_id)

    try:
        arrival_data, status_code = fetch_tfl_arrivals(line_id)

        object_key = save_to_s3(
            bucket_name=bucket_name,
            line_id=line_id,
            arrival_data=arrival_data,
            api_status_code=status_code,
        )

        logger.info(
            "Stored %s arrival records in s3://%s/%s",
            len(arrival_data),
            bucket_name,
            object_key,
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "TfL arrival data stored successfully",
                    "line_id": line_id,
                    "records_received": len(arrival_data),
                    "bucket": bucket_name,
                    "object_key": object_key,
                }
            ),
        }

    except Exception as error:
        logger.exception("TfL ingestion failed")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "TfL ingestion failed",
                    "error": str(error),
                }
            ),
        }