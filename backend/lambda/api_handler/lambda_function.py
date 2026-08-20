import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'ScrapedResultsTable')

def lambda_handler(event, context):
    """
    AWS Lambda handler triggered by API Gateway GET /search?query=laptops
    Reads structured metrics and items from DynamoDB.
    """
    print(f"API Handler Lambda started with event: {json.dumps(event)}")
    
    # 1. Extract query parameter
    query_params = event.get('queryStringParameters', {}) or {}
    query = query_params.get('query', '')
    
    # Fallback to direct event values if testing
    if not query:
        query = event.get('query', '')
        
    if not query:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Missing query parameter'})
        }
        
    print(f"Searching database for query: {query}")
    
    summary_record = {}
    cleaned_items = []
    
    # 2. Query DynamoDB Table
    try:
        table = dynamodb.Table(DYNAMODB_TABLE)
        
        # Query all records matching the Partition Key (SearchQuery)
        response = table.query(
            KeyConditionExpression=Key('SearchQuery').eq(query)
        )
        
        db_items = response.get('Items', [])
        print(f"Found {len(db_items)} items in DynamoDB")
        
        # Split summary record and raw records
        for item in db_items:
            if item.get('ItemId') == 'METRIC_SUMMARY':
                summary_record = item
            else:
                cleaned_items.append(item)
                
    except Exception as e:
        print(f"DynamoDB query failed (expected in local environment): {str(e)}")
        # Check local DB file fallback
        local_db_path = os.environ.get('LOCAL_DB_FILE')
        if local_db_path and os.path.exists(local_db_path):
            try:
                with open(local_db_path, 'r') as f:
                    local_db = json.load(f)
                
                query_data = local_db.get(query, {})
                summary_record = query_data.get('summary', {})
                cleaned_items = query_data.get('items', [])
                print(f"Loaded {len(cleaned_items)} items from local file database")
            except Exception as read_err:
                print(f"Error reading local DB: {str(read_err)}")
                
    # 3. Check if any results were found
    if not summary_record and not cleaned_items:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': f'No scraping data found for query: "{query}". Initiate scraping first.'
            })
        }
        
    # 4. Formulate Response payload
    response_payload = {
        'query': query,
        'queryType': summary_record.get('queryType', 'ecommerce'),
        'highlights': summary_record.get('highlights', []),
        'analytics': summary_record.get('analytics', {}),
        'sentiment': summary_record.get('sentiment', {}),
        'latency': summary_record.get('latency', 0.0),
        'payload_size': summary_record.get('payload_size', 0),
        'source_name': summary_record.get('source_name', 'Web Scraper'),
        'items': cleaned_items
    }

    def convert_decimals_to_floats(obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        elif isinstance(obj, dict):
            return {k: convert_decimals_to_floats(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_decimals_to_floats(v) for v in obj]
        return obj
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(convert_decimals_to_floats(response_payload))
    }
