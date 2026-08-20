import os
import sys
import json
import time
import uuid
import urllib.parse
import http.server
import socketserver
import threading
import queue
from unittest.mock import MagicMock

# ----------------------------------------------------------------------
# 1. Dependency Shield: Mock boto3, requests, and bs4 if not installed
# ----------------------------------------------------------------------
try:
    import boto3
except ImportError:
    print("[Simulator] boto3 not installed. Creating mock wrapper.")
    mock_boto3 = MagicMock()
    sys.modules['boto3'] = mock_boto3

try:
    import requests
except ImportError:
    print("[Simulator] requests not installed. Creating mock wrapper.")
    mock_req = MagicMock()
    sys.modules['requests'] = mock_req

try:
    import bs4
    from bs4 import BeautifulSoup
except ImportError:
    print("[Simulator] beautifulsoup4 not installed. Creating mock wrapper.")
    mock_bs4 = MagicMock()
    sys.modules['bs4'] = mock_bs4
    sys.modules['bs4.BeautifulSoup'] = MagicMock()

# Set mock environment variables for Lambdas
os.environ['RAW_S3_BUCKET'] = 'mock-raw-s3-bucket'
os.environ['DYNAMODB_TABLE'] = 'mock-scraped-results-table'
os.environ['LOCAL_S3_DIR'] = os.path.abspath('./mock_s3')
os.environ['LOCAL_DB_FILE'] = os.path.abspath('./mock_dynamodb.json')

# Create mock S3 directory
os.makedirs(os.environ['LOCAL_S3_DIR'], exist_ok=True)

# Add Lambda parent directory to Python path so we can import them as modules
sys.path.append(os.path.abspath('./backend/lambda'))

# Import Lambdas
try:
    import scraper.lambda_function as scraper_lambda
    import transformer.lambda_function as transformer_lambda
    import api_handler.lambda_function as api_handler_lambda
    print("[Simulator] Successfully loaded Lambda handlers.")
except ImportError as e:
    print(f"[Simulator] Error importing Lambdas: {e}")
    print("[Simulator] Please ensure directory layout matches structure.")

# ----------------------------------------------------------------------
# 2. Global Thread-Safe Queues & Event Bus State
# ----------------------------------------------------------------------
PORT = 5000
FRONTEND_DIR = os.path.abspath('./frontend')

# SQS Queue
sqs_queue = queue.Queue()

# Active schedules: { query: { "interval": seconds, "last_run": timestamp } }
schedules = {}
schedules_lock = threading.Lock()

# Connected Event Bus clients (SSE handlers)
event_bus_clients = []
clients_lock = threading.Lock()

# Alert Rules: [ { "id": str, "query": str, "criteria": "below" | "above", "val": float, "channel": str } ]
alert_rules = []
alert_rules_lock = threading.Lock()
ALERTS_FILE = os.path.abspath('./mock_alerts.json')

def load_alert_rules():
    global alert_rules
    if os.path.exists(ALERTS_FILE):
        try:
            with open(ALERTS_FILE, 'r') as f:
                alert_rules = json.load(f)
        except Exception:
            alert_rules = []
    else:
        alert_rules = []

def save_alert_rules():
    try:
        with open(ALERTS_FILE, 'w') as f:
            json.dump(alert_rules, f, indent=2)
    except Exception as e:
        print(f"Error saving alert rules: {e}")

# Load alert rules initially
load_alert_rules()

def run_alert_matching_engine(query, items, is_job):
    print(f"[Alert Engine] Checking {len(items)} items for query '{query}' against active rules...")
    triggered_alerts = []
    
    with alert_rules_lock:
        for rule in alert_rules:
            # Check if query matches rule query (case-insensitive substring match)
            if rule['query'].lower() not in query.lower():
                continue
                
            for item in items:
                # Get item name/title
                item_name = item.get('title', item.get('name', 'Unknown Listing'))
                
                # Extract numeric value
                val_to_check = 0.0
                if is_job:
                    val_to_check = item.get('salary_numeric', 0.0)
                    item_val_str = item.get('salary', 'N/A')
                else:
                    val_to_check = item.get('price_numeric', 0.0)
                    item_val_str = item.get('price', 'N/A')
                    
                rule_val = float(rule['val'])
                criteria = rule['criteria']
                
                triggered = False
                if criteria == 'below' and val_to_check > 0 and val_to_check < rule_val:
                    triggered = True
                elif criteria == 'above' and val_to_check > rule_val:
                    triggered = True
                    
                if triggered:
                    alert_msg = f"[{rule['channel'].upper()} ALERT] Match Found: '{item_name}' is {item_val_str} (Rule: {criteria} {rule['val']})"
                    print(f"[Alert Engine] ---> TRIGGERED: {alert_msg}")
                    triggered_alerts.append({
                        'rule_id': rule['id'],
                        'item_name': item_name,
                        'item_value': item_val_str,
                        'message': alert_msg,
                        'timestamp': time.strftime('%H:%M:%S')
                    })
                    # Send live toast event to client immediately
                    broadcast_event('toast', 'dynamo', 'completed', 'Alert Triggered', alert_msg, query)
                    break # Trigger once per rule per scrape
    return triggered_alerts

def broadcast_event(event_type, step, status, status_text, log_message, query, extra_data=None):
    """
    Sends JSON payload to all connected frontend clients on the /api/event-bus.
    """
    payload = {
        'event': event_type,        # 'progress', 'completed', 'log', 'toast'
        'step': step,              # 'gateway', 'sqs', 'scraper', 's3', 'transformer', 'dynamo'
        'status': status,          # 'active', 'completed', 'failed'
        'status_text': status_text,# 'Processing', 'Cleaned', etc.
        'log': log_message,
        'query': query,
        'timestamp': time.time()
    }
    if extra_data:
        payload['data'] = extra_data

    json_str = json.dumps(payload)
    message = f"data: {json_str}\n\n"

    with clients_lock:
        closed_clients = []
        for client in event_bus_clients:
            try:
                client.wfile.write(message.encode('utf-8'))
                client.wfile.flush()
            except Exception:
                closed_clients.append(client)
        
        # Clean up disconnected clients
        for client in closed_clients:
            if client in event_bus_clients:
                event_bus_clients.remove(client)

# ----------------------------------------------------------------------
# 3. Background Threads: SQS Worker & EventBridge Scheduler
# ----------------------------------------------------------------------
def sqs_worker():
    """
    Simulates the AWS SQS Queue executor. Polls tasks and runs the
    Lambda sequence one-by-one.
    """
    print("[SQS] Worker Thread started.")
    while True:
        try:
            job = sqs_queue.get()
            query = job.get('query')
            trigger_type = job.get('trigger_type', 'manual') # 'manual' or 'eventbridge'
            print(f"\n[SQS] ---> Processing task: '{query}' (Trigger: {trigger_type})")
            
            log_prefix = f"[EventBridge] " if trigger_type == 'eventbridge' else ""
            
            # --- STAGE 1: API Gateway ---
            broadcast_event('progress', 'gateway', 'completed', 'Success', 
                            f"{log_prefix}API Gateway authorized request. Dispatching SQS message.", query)
            time.sleep(0.8)
            
            # --- STAGE 2: SQS Queue ---
            broadcast_event('progress', 'sqs', 'active', 'Processing', 
                            f"{log_prefix}SQS: Job polled from queue. MessageId: {uuid.uuid4()}", query)
            time.sleep(0.8)
            broadcast_event('progress', 'sqs', 'completed', 'Active', 
                            f"{log_prefix}SQS: Queue visibility confirmed. Invoking Lambda Scraper.", query)
            
            # --- STAGE 3: Lambda Scraper Execution ---
            broadcast_event('progress', 'scraper', 'active', 'Scraping', 
                            f"{log_prefix}Lambda Scraper: Initiating web fetch...", query)
            time.sleep(1.2)
            
            scraper_event = {"Records": [{"body": json.dumps({"query": query})}]}
            scraper_response = scraper_lambda.lambda_handler(scraper_event, None)
            res_body = json.loads(scraper_response['body'])
            s3_key = res_body.get('s3_key', '')
            items_scraped = res_body.get('items_scraped', 0)
            
            broadcast_event('progress', 'scraper', 'completed', 'Success', 
                            f"{log_prefix}Lambda Scraper completed. Scraped {items_scraped} raw items.", query)
            
            # --- STAGE 4: S3 Bucket raw upload ---
            broadcast_event('progress', 's3', 'active', 'Uploading', 
                            f"{log_prefix}S3 Bucket: Uploading raw file 's3://mock-raw-s3-bucket/{s3_key}'", query)
            time.sleep(0.8)
            broadcast_event('progress', 's3', 'completed', 'Uploaded', 
                            f"{log_prefix}S3: ObjectCreated event notification emitted.", query)
            
            # --- STAGE 5: Lambda Transformer / Glue ---
            broadcast_event('progress', 'transformer', 'active', 'Transforming', 
                            f"{log_prefix}Lambda Transformer: Triggered by S3. Commencing Glue ETL clean rules...", query)
            time.sleep(1.2)
            
            transformer_event = {
                "bucket_name": os.environ['RAW_S3_BUCKET'],
                "s3_key": s3_key
            }
            transformer_response = transformer_lambda.lambda_handler(transformer_event, None)
            trans_body = json.loads(transformer_response['body'])
            items_processed = trans_body.get('items_processed', 0)
            
            broadcast_event('progress', 'transformer', 'completed', 'Cleaned', 
                            f"{log_prefix}Lambda Transformer: Stripped rates/prices. Generated AI Sentiment Insights (Comprehend simulation).", query)
            
            # --- STAGE 6: DynamoDB ---
            broadcast_event('progress', 'dynamo', 'active', 'Loading', 
                            f"{log_prefix}DynamoDB: Loading {items_processed} structured rows and 1 metadata summary record.", query)
            time.sleep(0.8)
            broadcast_event('progress', 'dynamo', 'completed', 'Success', 
                            f"{log_prefix}DynamoDB: Table write operations completed.", query)
            
            # --- FINAL: Fetch updated results ---
            api_event = {"query": query}
            api_response = api_handler_lambda.lambda_handler(api_event, None)
            final_data = json.loads(api_response['body'])
            
            # Run Alert Engine
            is_job = final_data.get('queryType') == 'jobs'
            triggered = run_alert_matching_engine(query, final_data.get('items', []), is_job)
            final_data['alerts'] = triggered
            
            # Broadcast completion to trigger frontend refresh
            broadcast_event('completed', 'dynamo', 'completed', 'Success', 
                            f"{log_prefix}Pipeline complete. Scraping & Transformation results updated in DynamoDB.", query, final_data)
            
            # --- Email Notification Sender (Real AWS SES with Fallback) ---
            email = job.get('email', '')
            if email:
                print(f"[SES] ---> Attempting to send actual email notification to {email} via AWS SES...")
                try:
                    # Retrieve region from env or fall back to ap-south-1 (from configured credentials)
                    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-south-1')
                    ses_client = boto3.client('ses', region_name=aws_region)
                    
                    # We use the recipient's email as both sender and receiver.
                    # This ensures that in the AWS SES sandbox mode, they only need to verify one email address.
                    response = ses_client.send_email(
                        Source=email,
                        Destination={'ToAddresses': [email]},
                        Message={
                            'Subject': {
                                'Data': f"JobPulse AWS: Scraper Alert for '{query}'"
                            },
                            'Body': {
                                'Text': {
                                    'Data': (
                                        f"Hello,\n\n"
                                        f"Your automated fresher job search for '{query}' completed successfully.\n"
                                        f"We mined {items_processed} jobs from Internshala and FreshersHunt.\n\n"
                                        f"Best regards,\n"
                                        f"JobPulse AWS Team"
                                    )
                                }
                            }
                        }
                    )
                    email_msg = f"[SES EMAIL SENDER] Actual email successfully sent via AWS SES to {email}! MessageId: {response['MessageId']}"
                    print(f"[SES] ---> {email_msg}")
                    broadcast_event('toast', 'dynamo', 'completed', 'SES Sent', email_msg, query)
                except Exception as ses_err:
                    print(f"[SES] Error: Send failed: {str(ses_err)}")
                    # Show simulated toast with a setup suggestion
                    email_msg = (
                        f"[SES EMAIL SENDER] Real email send to {email} failed ({str(ses_err)}). "
                        f"Please ensure this email is verified in your AWS SES Console (Region: ap-south-1)."
                    )
                    broadcast_event('toast', 'dynamo', 'completed', 'SES Simulated', email_msg, query)

                
            print(f"[SQS] <--- Finished task: '{query}'")
            sqs_queue.task_done()
        except Exception as e:
            print(f"[SQS] Worker Error: {e}")
            sqs_queue.task_done()

def eventbridge_scheduler():
    """
    Simulates AWS EventBridge Scheduler. Scans registered cron rules
    and enqueues background jobs into SQS accordingly.
    """
    print("[EventBridge] Scheduler Thread started.")
    while True:
        now = time.time()
        due_jobs = []
        
        with schedules_lock:
            for query, conf in schedules.items():
                interval = conf['interval']
                last_run = conf['last_run']
                if now - last_run >= interval:
                    due_jobs.append((query, conf.get('email', '')))
                    schedules[query]['last_run'] = now
                    
        for query, email in due_jobs:
            print(f"[EventBridge] Timer fired for '{query}'. Enqueueing scraping task to SQS.")
            # Trigger notification toast on UI
            broadcast_event('toast', 'gateway', 'active', 'Triggered', 
                            f"EventBridge Scheduler: Cron triggered background scrape for '{query}'!", query)
            sqs_queue.put({
                'query': query,
                'trigger_type': 'eventbridge',
                'email': email
            })
            
        time.sleep(1.0) # Check schedules every second

# Start workers
threading.Thread(target=sqs_worker, daemon=True).start()
threading.Thread(target=eventbridge_scheduler, daemon=True).start()

# ----------------------------------------------------------------------
# 4. Multi-Threaded HTTP Server
# ----------------------------------------------------------------------
class ThreadedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

class SimulatorHTTPHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=FRONTEND_DIR, **kwargs)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, DELETE')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        if parsed_url.path == '/api/event-bus':
            self.handle_event_bus()
        elif parsed_url.path == '/api/schedules':
            self.handle_get_schedules()
        elif parsed_url.path == '/api/queries':
            self.handle_get_queries()
        elif parsed_url.path == '/api/results':
            query = params.get('query', [''])[0]
            self.handle_get_results(query)
        elif parsed_url.path == '/api/alerts':
            self.handle_get_alerts()
        else:
            super().do_GET()

    def do_POST(self):
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        if parsed_url.path == '/api/search':
            query = params.get('query', [''])[0]
            self.handle_post_search(query)
        elif parsed_url.path == '/api/schedule':
            query = params.get('query', [''])[0]
            rate = params.get('rate', ['30'])[0] # seconds
            email = params.get('email', [''])[0] # recipient email
            self.handle_post_schedule(query, rate, email)
        elif parsed_url.path == '/api/unschedule':
            query = params.get('query', [''])[0]
            self.handle_post_unschedule(query)
        elif parsed_url.path == '/api/alert':
            query = params.get('query', [''])[0]
            criteria = params.get('criteria', [''])[0]
            val = params.get('val', [''])[0]
            channel = params.get('channel', [''])[0]
            self.handle_post_alert(query, criteria, val, channel)
        elif parsed_url.path == '/api/unschedule-alert':
            rule_id = params.get('id', [''])[0]
            self.handle_delete_alert(rule_id)
        else:
            self.send_response(404)
            self.end_headers()

    def handle_event_bus(self):
        """
        Establishes a persistent Server-Sent Events connection.
        Registers the handler in event_bus_clients to receive broadcasts.
        """
        print("[HTTP] Client connected to Event Bus.")
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.end_headers()

        # Send initial success connection packet
        json_connect = json.dumps({'event': 'connected', 'log': 'AWS Event Bus Simulator: Online'})
        self.wfile.write(f"data: {json_connect}\n\n".encode('utf-8'))
        self.wfile.flush()

        with clients_lock:
            event_bus_clients.append(self)

        # Keep thread alive to detect disconnection
        while True:
            try:
                # Send a comment heartbeat every 5 seconds to test socket health
                time.sleep(5.0)
                self.wfile.write(b": heartbeat\n\n")
                self.wfile.flush()
            except Exception:
                print("[HTTP] Client disconnected from Event Bus.")
                with clients_lock:
                    if self in event_bus_clients:
                        event_bus_clients.remove(self)
                break

    def handle_get_schedules(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        with schedules_lock:
            # Format schedules for JSON response
            schedules_list = []
            for query, conf in schedules.items():
                schedules_list.append({
                    'query': query,
                    'interval': conf['interval'],
                    'email': conf.get('email', '')
                })
        
        self.wfile.write(json.dumps(schedules_list).encode('utf-8'))

    def handle_get_queries(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        queries_list = []
        local_db_path = os.environ.get('LOCAL_DB_FILE')
        if local_db_path and os.path.exists(local_db_path):
            try:
                with open(local_db_path, 'r') as f:
                    local_db = json.load(f)
                for q, data in local_db.items():
                    summary = data.get('summary', {})
                    items = data.get('items', [])
                    queries_list.append({
                        'query': q,
                        'scraped_at': summary.get('scraped_at', 'Recently'),
                        'items_count': len(items)
                    })
            except Exception as e:
                print(f"Error reading local DB: {e}")
                
        self.wfile.write(json.dumps(queries_list).encode('utf-8'))

    def handle_get_results(self):
        pass # Stub, results handled by direct api call or below

    def handle_get_results(self, query):
        if not query:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing query parameter")
            return

        api_event = {"query": query}
        api_response = api_handler_lambda.lambda_handler(api_event, None)
        
        self.send_response(api_response['statusCode'])
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(api_response['body'].encode('utf-8'))

    def handle_post_search(self, query):
        if not query:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing query parameter")
            return

        print(f"[HTTP] Search enqueued to SQS: '{query}'")
        sqs_queue.put({
            'query': query,
            'trigger_type': 'manual'
        })
        
        self.send_response(202) # Accepted
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'queued', 'query': query}).encode('utf-8'))

    def handle_get_alerts(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        with alert_rules_lock:
            self.wfile.write(json.dumps(alert_rules).encode('utf-8'))

    def handle_post_alert(self, query, criteria, val, channel):
        if not query or not criteria or not val or not channel:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing parameters")
            return
        
        rule_id = uuid.uuid4().hex[:8]
        new_rule = {
            'id': rule_id,
            'query': query,
            'criteria': criteria, # 'below' or 'above'
            'val': float(val),
            'channel': channel # 'sms', 'email', 'slack'
        }
        
        with alert_rules_lock:
            alert_rules.append(new_rule)
            save_alert_rules()
            
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'created', 'rule': new_rule}).encode('utf-8'))

    def handle_delete_alert(self, rule_id):
        if not rule_id:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing id")
            return
            
        removed = False
        with alert_rules_lock:
            global alert_rules
            initial_len = len(alert_rules)
            alert_rules = [r for r in alert_rules if r['id'] != rule_id]
            if len(alert_rules) < initial_len:
                removed = True
                save_alert_rules()
                
        if removed:
            self.send_response(200)
        else:
            self.send_response(404)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'deleted', 'success': removed}).encode('utf-8'))

    def handle_post_schedule(self, query, rate_sec, email):
        if not query:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing query parameter")
            return
            
        try:
            interval = int(rate_sec)
        except ValueError:
            interval = 60 # Default to 1 minute

        print(f"[HTTP] Schedule requested: '{query}' every {interval}s (Email: {email})")
        
        with schedules_lock:
            schedules[query] = {
                'interval': interval,
                'last_run': time.time() - interval + 2, # Trigger first run in 2 seconds
                'email': email
            }

        cron_label = f"rate({interval} seconds)"
        broadcast_event('toast', 'gateway', 'completed', 'Scheduled', 
                        f"[EventBridge] Created automated cron rule for '{query}'! Rate: {cron_label}", query)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'scheduled', 'query': query, 'interval': interval, 'email': email}).encode('utf-8'))

    def handle_post_unschedule(self, query):
        if not query:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing query parameter")
            return

        print(f"[HTTP] Unschedule requested: '{query}'")
        
        with schedules_lock:
            if query in schedules:
                del schedules[query]
                removed = True
            else:
                removed = False

        if removed:
            broadcast_event('toast', 'gateway', 'completed', 'Unscheduled', 
                            f"[EventBridge] Deleted cron rule for '{query}'.", query)
            self.send_response(200)
        else:
            self.send_response(404)
        
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'un-scheduled', 'query': query, 'success': removed}).encode('utf-8'))

# ----------------------------------------------------------------------
# 5. Main Execution
# ----------------------------------------------------------------------
if __name__ == '__main__':
    print("=" * 70)
    print("      AWS SERVERLESS WEB SCRAPER & ANALYTICS PIPELINE SIMULATOR")
    print("=" * 70)
    print(f"Mock S3 Storage Path:       {os.environ['LOCAL_S3_DIR']}")
    print(f"Mock DynamoDB Database:     {os.environ['LOCAL_DB_FILE']}")
    print("-" * 70)
    print(f"Server starting at: http://localhost:{PORT}")
    print("To open the UI, load http://localhost:5000/index.html in your browser.")
    print("Press Ctrl+C to stop.")
    print("=" * 70)

    # Use Threaded HTTPServer to allow SSE streaming without blocking HTTP loops
    httpd = ThreadedHTTPServer(("", PORT), SimulatorHTTPHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")
        sys.exit(0)
