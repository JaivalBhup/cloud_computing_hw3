import json
import logging
import boto3
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Clients
s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')
_os_client = None

allowed_image_xtensions = ['jpg', 'jpeg', 'png', 'gif']
def lambda_handler(event, context):
    try:
        logger.info("LF1 invoked")
        record = event['Records'][0]
        record_time =  record['eventTime']
        record_event_name = record['eventName']
        record_s3 = record['s3']
        bucket = record_s3['bucket']['name']
        key = record_s3['object']['key']
        
        if not record_event_name.startswith("ObjectCreated"):
            logger.warning(f"Ignoring event: {record_event_name}")
            return ok()
            
        if key.split('.')[-1].lower() not in allowed_image_xtensions:
            logger.info(f"Skipping non-image file: {key}")
            return ok()
        
        logger.info(f"Processing image: s3://{bucket}/{key}")
        
        head_object = get_head_object(bucket, key)
        if not head_object:
            logger.error("Failed to retrieve head_object, skipping processing")
            return ok()
        metadata = head_object.get("Metadata", {})
        
        custom_labels = []
        if "customlabels" in metadata:
            raw = metadata["customlabels"]
            custom_labels = [x.strip() for x in raw.split(",") if x.strip()]
            logger.info(f"Custom labels: {custom_labels}")

        detected_labels = detect_labels(bucket, key)

        all_labels = detected_labels + custom_labels
        
        custom_json_object = {
            "objectKey": key,
            "bucket": bucket,
            "createdTimestamp": head_object["LastModified"].isoformat(),
            "labels": all_labels
        }
        
        logger.info("Final combined label object constructed.")
        logger.info(custom_json_object)
        # logger.info("Indexing the document")
        # index_document(custom_json_object)
        return ok()
    
    except Exception as e:
        logger.exception(f"Unhandled Lambda error: {e}")
        return error("Unhandled error")


def get_head_object(bucket, key):
    try:
        return s3_client.head_object(
            Bucket=bucket,
            Key=key
        )
    except Exception as e:
        code = e.response["Error"]["Code"]
        logger.error(f"S3 head_object error ({code}): {e}")
        return None

def detect_labels(bucket, key):
    try:
        response = rekognition_client.detect_labels(
            Image={'S3Object':{'Bucket':bucket,'Name':key}},
            MaxLabels=10
        )

        labels = [label["Name"] for label in response["Labels"]]
        logger.info(f"Detected labels: {labels}")
        return labels

    except Exception as e:
        logger.error(f"Rekognition error for {key}: {e}")
        return []

def index_document(document):
    """
    document example:
    {
        "objectKey": "images/photo.jpg",
        "bucket": "mybucket",
        "createdTimestamp": "2025-02-01T10:00:00Z",
        "labels": ["Cat", "Animal", "Pet"]
    }
    """
    client = get_opensearch_client()
    index_name = os.environ["OPENSEARCH_INDEX"]

    try:
        # OpenSearch auto-creates index unless blocked by policy
        response = client.index(
            index=index_name,
            body=document,
            refresh=True  # so data appears instantly
        )

        logger.info(f"Indexed into OpenSearch: {response}")
        return True

    except Exception as e:
        logger.error(f"Failed to index document: {e}")
        return False


def get_opensearch_client():
    global _os_client

    if _os_client:
        return _os_client

    session = boto3.Session()
    credentials = session.get_credentials()
    region = session.region_name
    
    host = os.environ["OPENSEARCH_HOST"]

    service = "es"

    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token
    )

    _os_client = OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=10,
        max_retries=3,
        retry_on_timeout=True
    )

    return _os_client

def ok():
    return {"statusCode": 200, "body": "OK"}

def error(msg):
    return {"statusCode": 500, "body": msg}

