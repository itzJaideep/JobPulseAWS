import os
import sys
import json
import uuid
import unittest

# Configure Mock Environment variables
os.environ['RAW_S3_BUCKET'] = 'test-raw-s3-bucket'
os.environ['DYNAMODB_TABLE'] = 'test-scraped-results-table'
os.environ['LOCAL_S3_DIR'] = os.path.abspath('./test_s3')
os.environ['LOCAL_DB_FILE'] = os.path.abspath('./test_dynamodb.json')

# Create directories
os.makedirs(os.environ['LOCAL_S3_DIR'], exist_ok=True)

# Add paths
sys.path.append(os.path.abspath('./backend/lambda'))

try:
    import scraper.lambda_function as scraper_lambda
    import transformer.lambda_function as transformer_lambda
    import api_handler.lambda_function as api_handler_lambda
    print("Test Runner: Loaded Lambda modules successfully.\n")
except ImportError as e:
    print(f"Test Runner: Import Error: {e}")
    sys.exit(1)

class TestScraperPipeline(unittest.TestCase):
    def setUp(self):
        # Clear mock files before each test
        if os.path.exists(os.environ['LOCAL_DB_FILE']):
            os.remove(os.environ['LOCAL_DB_FILE'])
        for f in os.listdir(os.environ['LOCAL_S3_DIR']):
            os.remove(os.path.join(os.environ['LOCAL_S3_DIR'], f))

    def test_e2e_ecommerce_flow(self):
        print("=" * 60)
        print("BLACK BOX TEST: E-Commerce Search ('mechanical keyboard')")
        print("=" * 60)
        
        # 1. Scraper
        scraper_event = {"query": "mechanical keyboard"}
        print("1. Invoking Scraper...")
        scraper_res = scraper_lambda.lambda_handler(scraper_event, None)
        self.assertEqual(scraper_res['statusCode'], 200)
        
        body = json.loads(scraper_res['body'])
        s3_key = body['s3_key']
        items_scraped = body['items_scraped']
        print(f"   Success. Items Scraped: {items_scraped}. S3 Key: {s3_key}")
        
        # Verify file exists
        local_s3_file = os.path.join(os.environ['LOCAL_S3_DIR'], s3_key.replace('/', '_'))
        self.assertTrue(os.path.exists(local_s3_file))
        
        with open(local_s3_file, 'r') as f:
            raw_data = json.load(f)
        self.assertEqual(raw_data['query'], "mechanical keyboard")
        self.assertEqual(len(raw_data['items']), items_scraped)
        
        # Verify keys in raw item
        if items_scraped > 0:
            first_item = raw_data['items'][0]
            self.assertIn('name', first_item)
            self.assertIn('price', first_item)
            self.assertIn('rating', first_item)
            print("   Raw Item Schema matches e-commerce specifications.")
        
        # 2. Transformer
        print("2. Invoking Transformer...")
        trans_event = {
            "bucket_name": os.environ['RAW_S3_BUCKET'],
            "s3_key": s3_key
        }
        trans_res = transformer_lambda.lambda_handler(trans_event, None)
        self.assertEqual(trans_res['statusCode'], 200)
        
        trans_body = json.loads(trans_res['body'])
        items_processed = trans_body['items_processed']
        print(f"   Success. Items Processed: {items_processed}")
        
        # Verify DynamoDB (local file) contains the items and summary record
        self.assertTrue(os.path.exists(os.environ['LOCAL_DB_FILE']))
        with open(os.environ['LOCAL_DB_FILE'], 'r') as f:
            db_data = json.load(f)
            
        self.assertIn("mechanical keyboard", db_data)
        query_records = db_data["mechanical keyboard"]
        self.assertIn('summary', query_records)
        self.assertEqual(len(query_records['items']), items_processed)
        
        summary = query_records['summary']
        self.assertEqual(summary['ItemId'], 'METRIC_SUMMARY')
        self.assertEqual(summary['queryType'], 'ecommerce')
        self.assertIn('avg_price', summary)
        self.assertIn('avg_rating', summary)
        self.assertIn('sentiment', summary)
        self.assertIn('highlights', summary)
        self.assertIn('analytics', summary)
        print("   ETL Output: Metric summary record validated successfully.")
        
        # Verify details of transformed e-commerce item
        first_clean_item = query_records['items'][0]
        self.assertIn('price_numeric', first_clean_item)
        self.assertIn('brand', first_clean_item)
        self.assertIn('stock', first_clean_item)
        
        # 3. API Handler
        print("3. Invoking API Handler...")
        api_event = {"query": "mechanical keyboard"}
        api_res = api_handler_lambda.lambda_handler(api_event, None)
        self.assertEqual(api_res['statusCode'], 200)
        
        api_body = json.loads(api_res['body'])
        self.assertEqual(api_body['query'], "mechanical keyboard")
        self.assertEqual(api_body['queryType'], "ecommerce")
        self.assertEqual(len(api_body['items']), items_processed)
        print("   API Handler Response: Formatted dashboard data payload matches specifications.")
        print("-" * 60)

    def test_e2e_jobs_flow(self):
        print("=" * 60)
        print("BLACK BOX TEST: Jobs Search ('devops engineer')")
        print("=" * 60)
        
        # 1. Scraper
        scraper_event = {"query": "devops engineer"}
        print("1. Invoking Scraper...")
        scraper_res = scraper_lambda.lambda_handler(scraper_event, None)
        self.assertEqual(scraper_res['statusCode'], 200)
        
        body = json.loads(scraper_res['body'])
        s3_key = body['s3_key']
        items_scraped = body['items_scraped']
        print(f"   Success. Items Scraped: {items_scraped}. S3 Key: {s3_key}")
        
        # 2. Transformer
        print("2. Invoking Transformer...")
        trans_event = {
            "bucket_name": os.environ['RAW_S3_BUCKET'],
            "s3_key": s3_key
        }
        trans_res = transformer_lambda.lambda_handler(trans_event, None)
        self.assertEqual(trans_res['statusCode'], 200)
        
        trans_body = json.loads(trans_res['body'])
        items_processed = trans_body['items_processed']
        print(f"   Success. Items Processed: {items_processed}")
        
        # Verify local db contains jobs summary
        with open(os.environ['LOCAL_DB_FILE'], 'r') as f:
            db_data = json.load(f)
            
        query_records = db_data["devops engineer"]
        summary = query_records['summary']
        self.assertEqual(summary['queryType'], 'jobs')
        
        # Verify details of transformed jobs item
        first_clean_item = query_records['items'][0]
        self.assertIn('title', first_clean_item)
        self.assertIn('company', first_clean_item)
        self.assertIn('location', first_clean_item)
        self.assertIn('salary_numeric', first_clean_item)
        print("   ETL Output: Clean jobs item schema validated.")
        
        # 3. API Handler
        print("3. Invoking API Handler...")
        api_event = {"query": "devops engineer"}
        api_res = api_handler_lambda.lambda_handler(api_event, None)
        self.assertEqual(api_res['statusCode'], 200)
        
        api_body = json.loads(api_res['body'])
        self.assertEqual(api_body['queryType'], "jobs")
        print("   API Handler Response: Formatted jobs dataset verified.")
        print("-" * 60)

if __name__ == '__main__':
    unittest.main()
