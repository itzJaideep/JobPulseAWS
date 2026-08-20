import json
import os
import uuid
import boto3
import re
import time
from decimal import Decimal

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'ScrapedResultsTable')

def clean_numeric_string(val_str):
    cleaned = "".join(c for c in val_str if c.isdigit() or c == '.')
    cleaned = cleaned.lstrip('.')
    cleaned = cleaned.rstrip('.')
    return cleaned

def lambda_handler(event, context):
    """
    AWS Lambda handler for cleaning and transforming raw scraped data from S3.
    """
    print(f"Transformer Lambda started with event: {json.dumps(event)}")
    
    # 1. Parse bucket name and key
    bucket_name = ""
    s3_key = ""
    
    if 'Records' in event and 's3' in event['Records'][0]:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
    else:
        bucket_name = event.get('bucket_name', 'aws-scraper-raw-data-bucket')
        s3_key = event.get('s3_key', '')
        
    if not s3_key:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No S3 key provided'})
        }
        
    # 2. Retrieve raw JSON file from S3
    print(f"Fetching raw file: s3://{bucket_name}/{s3_key}")
    raw_data = {}
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        raw_data = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"S3 Retrieval failed (expected in local environments): {str(e)}")
        # Check local S3 directory fallback
        local_s3_path = os.environ.get('LOCAL_S3_DIR')
        if local_s3_path:
            filename = s3_key.replace('/', '_')
            filepath = os.path.join(local_s3_path, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    raw_data = json.load(f)
                print(f"Successfully loaded raw data from local folder: {filepath}")
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': f"Local file not found: {filepath}"})
                }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f"Could not retrieve S3 object: {str(e)}"})
            }
            
    query = raw_data.get('query', '')
    raw_items = raw_data.get('items', [])
    
    if not query:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Raw file missing query'})
        }
        
    print(f"Transforming {len(raw_items)} items for query: {query}")
    
    # 3. Clean and Transform Data
    cleaned_items = []
    
    # Force query to strictly use job transformation logic
    is_job = True
    
    prices_list = []
    ratings_list = []
    wlb_list = []
    sal_list = []
    
    for item in raw_items:
        cleaned_item = {
            'ItemId': str(uuid.uuid4()),
            'SearchQuery': query,
            'link': item.get('link', ''),
            'source': item.get('source', 'Web Scraper')
        }
        
        # Clean specific fields
        if is_job:
            title = item.get('title', 'Unknown Job')
            company = item.get('company', 'Unknown Company')
            location = item.get('location', 'Remote')
            salary_str = item.get('salary', '₹3,00,000 /yr')
            rating_str = item.get('rating', '3.8')
            
            # Normalizes Indian salary string into Lakhs Per Annum (LPA)
            salary_val = 0.0
            try:
                sal_clean = salary_str.lower()
                is_monthly = 'month' in sal_clean or 'pm' in sal_clean
                
                # Check for range
                if '-' in sal_clean or ' - ' in sal_clean:
                    parts = re.split(r'[-\u2013\u2014]', sal_clean)
                    vals = []
                    for p in parts:
                        p = p.strip()
                        mult = 1.0
                        if 'k' in p:
                            mult = 1000.0
                        elif 'l' in p:
                            mult = 100000.0
                        num_part = clean_numeric_string(p)
                        if num_part:
                            vals.append(float(num_part) * mult)
                    raw_val = sum(vals) / len(vals) if vals else 0.0
                else:
                    mult = 1.0
                    if 'k' in sal_clean:
                        mult = 1000.0
                    elif 'l' in sal_clean:
                        mult = 100000.0
                    num_part = clean_numeric_string(sal_clean)
                    raw_val = float(num_part) * mult if num_part else 0.0
                
                if is_monthly:
                    salary_val = (raw_val * 12) / 100000.0
                else:
                    if raw_val > 1000:
                        salary_val = raw_val / 100000.0
                    else:
                        salary_val = raw_val
            except Exception:
                pass
                
            posted = item.get('posted', 'Just now')
            cleaned_item.update({
                'title': title,
                'company': company,
                'location': location,
                'salary': salary_str,
                'salary_numeric': salary_val,
                'rating': rating_str,
                'posted': posted,
                'description': item.get('description', '')[:300] + '...'
            })
            if salary_val > 0:
                prices_list.append(salary_val)
                
            try:
                ratings_list.append(float(rating_str))
            except ValueError:
                ratings_list.append(3.8)
        else:
            # Company Reviews
            reviewer = item.get('reviewer', 'Anonymous Employee')
            rating_str = item.get('rating', '4.0')
            wlb_str = item.get('workload_rating', '3.0')
            sal_str = item.get('salary_rating', '3.0')
            review_text = item.get('review', '')
            
            try:
                r_val = float(rating_str)
                w_val = float(wlb_str)
                s_val = float(sal_str)
            except ValueError:
                r_val, w_val, s_val = 4.0, 3.0, 3.0
                
            cleaned_item.update({
                'reviewer': reviewer,
                'rating': rating_str,
                'workload_rating': wlb_str,
                'salary_rating': sal_str,
                'review': review_text[:300] + '...' if len(review_text) > 300 else review_text
            })
            
            ratings_list.append(r_val)
            wlb_list.append(w_val)
            sal_list.append(s_val)
            
        cleaned_items.append(cleaned_item)
        
    # 4. Generate Aggregated Insights & Sentiment Analysis (Simulating AWS Comprehend/Glue summaries)
    positive_words = ['excellent', 'best', 'great', 'fantastic', 'amazing', 'highly', 'flexible', 'love', 'easy', 'satisfied', 'remote', 'bonus', 'growth', 'unlimited', 'top', 'supportive']
    negative_words = ['bad', 'worst', 'poor', 'defect', 'issue', 'problem', 'broken', 'slow', 'disappointed', 'complaint', 'difficult', 'charge', 'stressful', 'busy', 'pressure', 'politics']
    
    pos_sentiment_count = 0
    neg_sentiment_count = 0
    neu_sentiment_count = 0
    
    extracted_keywords = set()
    
    # Pre-defined entities to extract (representing AWS Comprehend Entities)
    tech_entities = ['aws', 'kubernetes', 'docker', 'terraform', 'python', 'react', 'typescript', 'java', 'node', 'ci/cd', 'git', 'devops', 'linux', 'cloud', 'hr', 'sales', 'finance', 'ai', 'chatgpt']
    
    for item in raw_items:
        desc = (item.get('description', '') if is_job else item.get('review', '')).lower()
        
        # Classify sentiment by keyword occurrence
        pos_hits = sum(1 for w in positive_words if w in desc)
        neg_hits = sum(1 for w in negative_words if w in desc)
        
        rating = float(item.get('rating', '4.0'))
        if rating >= 4.5:
            pos_hits += 2
        elif rating < 3.2:
            neg_hits += 2
            
        if pos_hits > neg_hits:
            pos_sentiment_count += 1
        elif neg_hits > pos_hits:
            neg_sentiment_count += 1
        else:
            neu_sentiment_count += 1
            
        # Extract entities/keywords
        for ent in tech_entities:
            if ent in desc or ent in item.get('title', '').lower():
                extracted_keywords.add(ent.upper())
                
    total_items = len(raw_items) or 1
    sentiment = {
        'positive': int((pos_sentiment_count / total_items) * 100),
        'neutral': int((neu_sentiment_count / total_items) * 100),
        'negative': int((neg_sentiment_count / total_items) * 100),
        'keywords': list(extracted_keywords)[:8] if extracted_keywords else (['HR', 'SALES', 'TECH', 'WORK-LIFE'] if not is_job else ['PYTHON', 'REMOTE', 'CI/CD']),
        'summary': f"Analysis based on {total_items} live source items. Extracted entities: {', '.join(list(extracted_keywords)[:4])}."
    }
    
    # Compute highlights and charts
    highlights = []
    analytics = {}
    
    if is_job:
        avg_price = sum(prices_list) / len(prices_list) if prices_list else 0.0
        avg_rating = sum(ratings_list) / len(ratings_list) if ratings_list else 3.8
        highest_job = max(cleaned_items, key=lambda x: x.get('salary_numeric', 0.0), default=None)
        highest_pay = highest_job['salary'] if highest_job else "N/A"
        highest_title = highest_job['title'] if highest_job else "N/A"
        
        highlights = [
            { 'title': 'Max Compensation', 'value': highest_pay, 'desc': highest_title, 'icon': 'fa-wallet', 'colorClass': 'purple' },
            { 'title': 'Average Annual Salary', 'value': f"₹{avg_price:.2f} Lakhs/yr" if avg_price > 0 else "N/A", 'desc': 'Across all matching entries', 'icon': 'fa-calculator', 'colorClass': 'blue' },
            { 'title': 'Market Quality Score', 'value': f"{avg_rating:.1f} / 5.0", 'desc': 'Average company rating', 'icon': 'fa-star', 'colorClass': 'emerald' }
        ]
        
        # Salaries distribution
        analytics = {
            'labels': ['Entry Level (<₹3L/yr)', 'Mid-Level (₹3L-6L/yr)', 'Senior (₹6L-12L/yr)', 'Lead (₹12L+/yr)'],
            'values': [
                len([c for c in cleaned_items if c.get('salary_numeric', 0) < 3.0]),
                len([c for c in cleaned_items if 3.0 <= c.get('salary_numeric', 0) < 6.0]),
                len([c for c in cleaned_items if 6.0 <= c.get('salary_numeric', 0) < 12.0]),
                len([c for c in cleaned_items if c.get('salary_numeric', 0) >= 12.0])
            ]
        }
        avg_price_for_db = avg_price
    else:
        # Company Reviews
        avg_rating = sum(ratings_list) / len(ratings_list) if ratings_list else 3.8
        avg_wlb = sum(wlb_list) / len(wlb_list) if wlb_list else 3.0
        avg_sal = sum(sal_list) / len(sal_list) if sal_list else 3.0
        
        highlights = [
            { 'title': 'Overall Employer Rating', 'value': f"{avg_rating:.1f} / 5.0", 'desc': 'Average overall satisfaction', 'icon': 'fa-star', 'colorClass': 'emerald' },
            { 'title': 'Workload Rating', 'value': f"{avg_wlb:.1f} / 5.0", 'desc': 'Work-life balance rating', 'icon': 'fa-stopwatch', 'colorClass': 'purple' },
            { 'title': 'Salary review rating', 'value': f"{avg_sal:.1f} / 5.0", 'desc': 'Employee salary satisfaction', 'icon': 'fa-wallet', 'colorClass': 'blue' }
        ]
        
        # Ratings distribution counts
        counts = [0, 0, 0, 0, 0] # 5, 4, 3, 2, 1 star
        for r in ratings_list:
            if r >= 4.5:
                counts[0] += 1
            elif r >= 3.5:
                counts[1] += 1
            elif r >= 2.5:
                counts[2] += 1
            elif r >= 1.5:
                counts[3] += 1
            else:
                counts[4] += 1
                
        analytics = {
            'labels': ['5 Stars', '4 Stars', '3 Stars', '2 Stars', '1 Star'],
            'values': counts
        }
        avg_price_for_db = avg_rating # Stand-in mapping for the metrics
        
    # 5. Save Structured Results to DynamoDB (or mock JSON file for local testing)
    summary_record = {
        'SearchQuery': query,
        'ItemId': 'METRIC_SUMMARY',
        'queryType': 'jobs' if is_job else 'reviews',
        'avg_price': str(avg_price_for_db),
        'avg_rating': str(avg_rating),
        'sentiment': sentiment,
        'highlights': highlights,
        'analytics': analytics,
        'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'latency': raw_data.get('latency', 0.0),
        'payload_size': raw_data.get('payload_size', 0),
        'source_name': raw_items[0].get('source', 'Web Scraper') if raw_items else 'No Source'
    }
    
    print(f"Writing data to DynamoDB Table: {DYNAMODB_TABLE}")
    try:
        def convert_floats_to_decimals(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats_to_decimals(v) for v in obj]
            return obj
 
        table = dynamodb.Table(DYNAMODB_TABLE)
        
        # Write the Summary Record
        table.put_item(Item=convert_floats_to_decimals(summary_record))
        
        # Write individual items
        with table.batch_writer() as batch:
            for item in cleaned_items:
                batch.put_item(Item=convert_floats_to_decimals(item))
                
    except Exception as e:
        print(f"DynamoDB operations failed (expected in local environment): {str(e)}")
        # Check local DB fallback
        local_db_path = os.environ.get('LOCAL_DB_FILE')
        if local_db_path:
            # Store ONLY the current active query, completely purging all past historical query records
            local_db = {
                query: {
                    'summary': summary_record,
                    'items': cleaned_items
                }
            }
            
            with open(local_db_path, 'w') as f:
                json.dump(local_db, f, indent=2)
            print(f"Overwrote local file database with the active query (no past history saved): {local_db_path}")
 
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Data transformation and loading succeeded',
            'query': query,
            'items_processed': len(cleaned_items)
        })
    }
