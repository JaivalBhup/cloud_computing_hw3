import json
import logging
import boto3
import os
import uuid
import requests
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Pipline to search photos based on user query using AWS Lex and OpenSearch
lex_client = boto3.client("lexv2-runtime")
def lambda_handler(event, context):
    query_text = None
    if 'queryStringParameters' in event and event['queryStringParameters'] is not None:
        query_text = event['queryStringParameters'].get('q')
    logger.info(query_text)
    if query_text:
        response = lex_client.recognize_text(
            botId=os.environ['LEX_BOT_ID'],
            botAliasId=os.environ['LEX_BOT_ALIAS_ID'],
            localeId="en_US",
            text=query_text,
            sessionId=str(uuid.uuid4())
        )
        slots = response['sessionState']['intent']['slots']
        print(slots)
        keywords = [slots[keyword]["value"]["interpretedValue"] for keyword in slots.keys() if slots[keyword]]
        logger.info("Keywords interpreted:" +str(keywords))
        # Use response to search using open search
        # results = []
        results = search_with_opensearch(keywords)
        response_body = "Searching for keywords: "+str(keywords)
        status_code = 200
        print(keywords)
    else:
        response_body = "No search text provided."
        results = []
        status_code = 400

    return {
        'statusCode': status_code,
         'headers': {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS"
        },
        'body': json.dumps({'message': response_body, 'results': results })
    }

def search_with_opensearch(labels):
    session = boto3.Session()
    credentials = session.get_credentials()
    region = session.region_name
    OPENSEARCH_HOST = os.environ["OPENSEARCH_HOST"]  
    OPENSEARCH_INDEX = os.environ["OPENSEARCH_INDEX"]  
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        "es",
        session_token=credentials.token
    )

    # Example object stored in OpenSearch
        #     custom_json_object = {
        #     "objectKey": key,
        #     "bucket": bucket,
        #     "createdTimestamp": head_object["LastModified"].isoformat(),
        #     "labels": all_labels
        # }
    query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"labels": label}} for label in labels
                ]
            }
        }
    }
    headers = {"Content-Type": "application/json"}
    res = requests.get(
        f"https://{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_search", 
        headers=headers, 
        auth=awsauth,
        data=json.dumps(query)
    )
    try:
        res_json = res.json()
        logger.info(f"OpenSearch response: {res_json}")
    except ValueError:
        raise Exception(res.text)

    hits = res_json.get("hits", {}).get("hits", [])
    return [
        f"https://{hit['_source']['bucket']}.s3.us-east-1.amazonaws.com/{hit['_source']['objectKey']}"
        for hit in hits
    ]