import boto3
import csv
import logging
from io import StringIO
import json
from datetime import datetime, timezone

# Set up logging for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

EXPECTED_COLUMNS = ['Rank', 'Name', 'Platform', 'Year', 'Genre', 'Publisher', 'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']

s3_client = boto3.client('s3')
REPORT_BUCKET = "YOUR_RESULTS_BUCKET_NAME"  # Replace with your bucket name when deploying

def process_csv(file_content):
    reader = csv.DictReader(StringIO(file_content))
    
    # Check if the headers match what we expect
    if reader.fieldnames != EXPECTED_COLUMNS:
        logger.warning(f"Found headers: {reader.fieldnames}")
        logger.warning(f"Expected: {EXPECTED_COLUMNS}")
        # Continue anyway, might be a header format issue
    
    row_count = 0
    total_global_sales = 0.0
    publishers = set()
    genres = {}
    platforms = {}
    
    for row in reader:
        try:
            # Process the sales data
            global_sales = float(row['Global_Sales'])
            total_global_sales += global_sales
            
            # Collect publisher information
            if row['Publisher']:
                publishers.add(row['Publisher'])
            
            # Track genre popularity
            genre = row['Genre']
            if genre in genres:
                genres[genre] += 1
            else:
                genres[genre] = 1
                
            # Track platform popularity
            platform = row['Platform']
            if platform in platforms:
                platforms[platform] += 1
            else:
                platforms[platform] = 1
            
            row_count += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"Error processing row: {row}")
            logger.warning(f"Error details: {str(e)}")
    
    # Sort genres and platforms by popularity
    top_genres = sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5]
    top_platforms = sorted(platforms.items(), key=lambda x: x[1], reverse=True)[:5]
    
    average_sales = total_global_sales / row_count if row_count else 0
    return {
        "rows_processed": row_count,
        "total_global_sales": total_global_sales,
        "average_sales": average_sales,
        "unique_publishers": len(publishers),
        "top_genres": top_genres,
        "top_platforms": top_platforms
    }

def lambda_handler(event, context):
    
    # Extract bucket and file name from event
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
    except (KeyError, IndexError) as e:
        logger.error(f"Malformed S3 event: {str(e)}")
        return

    logger.info(f"Processing file: s3://{bucket}/{key}")

    try:
        # Download file content
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')

        # Process CSV content
        result = process_csv(file_content)
        logger.info(f"Processed results: {result}")
        
        # Store results in a dedicated bucket
        result_key = f"{key.split('/')[-1].replace('.csv', '')}_analysis.json"
        s3_client.put_object(
            Bucket=REPORT_BUCKET,
            Key=result_key,
            Body=json.dumps(result, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Results saved to s3://{REPORT_BUCKET}/{result_key}")
        
        # Generate and upload HTML report
        html_url = upload_html_report(result, key.split('/')[-1])
        logger.info(f"HTML report available at: {html_url}")
        
        # Update dashboard data with the new report
        dashboard_url = update_dashboard_data(result, html_url, key.split('/')[-1])
        logger.info(f"Dashboard updated and available at: {dashboard_url}")
        
        return {
            'statusCode': 200,
            'body': result,
            'html_report_url': html_url,
            'dashboard_url': dashboard_url
        }

    except Exception as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error processing file: {str(e)}"
        }
    
def upload_html_report(summary, original_key):
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    report_key = f'reports/{original_key}-{timestamp}.html'

    html_content = f"""
    <html>
    <head><title>CSV Report</title></head>
    <body>
        <h2>CSV Processing Report</h2>
        <ul>
            <li><strong>File:</strong> {original_key}</li>
            <li><strong>Processed at (UTC):</strong> {timestamp}</li>
            <li><strong>Rows:</strong> {summary['rows_processed']}</li>
            <li><strong>Total Global Sales:</strong> {summary['total_global_sales']:.2f}</li>
            <li><strong>Average Sales:</strong> {summary['average_sales']:.2f}</li>
            <li><strong>Unique Publishers:</strong> {summary['unique_publishers']}</li>
        </ul>
    </body>
    </html>
    """

    s3_client.put_object(
        Bucket=REPORT_BUCKET,
        Key=report_key,
        Body=html_content,
        ContentType='text/html'
    )

    return f"https://YOUR_CLOUDFRONT_DISTRIBUTION.cloudfront.net/{report_key}"

def update_dashboard_data(new_report, report_url, original_key):
    """Update the dashboard data with new report information"""
    
    # First try to get existing dashboard data
    try:
        existing_data_obj = s3_client.get_object(Bucket=REPORT_BUCKET, Key='dashboard/dashboard-data.json')
        dashboard_data = json.loads(existing_data_obj['Body'].read().decode('utf-8'))
    except:
        # If no existing data, create a new structure
        dashboard_data = {
            'reports': [],
            'latest_stats': None
        }
    
    # Generate timestamp for the report ID
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    
    # Add the new report to the list (keeping only the 10 most recent)
    report_info = {
        'reportId': f"{original_key}-{timestamp}",
        'url': report_url,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'filename': original_key,
        'rows_processed': new_report['rows_processed']
    }
    
    dashboard_data['reports'].insert(0, report_info)
    dashboard_data['reports'] = dashboard_data['reports'][:10]  # Keep only 10 most recent
    
    # Update the latest stats
    dashboard_data['latest_stats'] = new_report
    
    # Save the updated dashboard data
    s3_client.put_object(
        Bucket=REPORT_BUCKET,
        Key='dashboard-data.json',
        Body=json.dumps(dashboard_data, indent=2),
        ContentType='application/json'
    )
    
    # Also upload the dashboard HTML if it doesn't exist
    try:
        s3_client.head_object(Bucket=REPORT_BUCKET, Key='dashboard/index.html')
    except:
        pass
    
    return f"https://YOUR_CLOUDFRONT_DISTRIBUTION.cloudfront.net"