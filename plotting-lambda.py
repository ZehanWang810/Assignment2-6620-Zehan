import json
import boto3
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')  # Required for Lambda environment
import matplotlib.pyplot as plt
import io
import matplotlib.dates as mdates

def lambda_handler(event, context):
    """
    Lambda function to plot S3 bucket size changes and save to S3.
    Triggered via API Gateway.
    
    Requirements:
    - DynamoDB table 'S3-object-size-history' with:
        - Partition key: bucket_name (string)
        - Sort key: timestamp (number/string)
        - Attributes: total_size (number), object_count (number)
    - Lambda execution role must have permissions for:
        - dynamodb:Query
        - s3:PutObject
    """
    
    # Initialize AWS clients
    dynamodb = boto3.resource('dynamodb')
    s3 = boto3.client('s3')
    table = dynamodb.Table('S3-object-size-history')
    
    try:
        # Calculate time range (last 10 seconds)
        now = datetime.utcnow()
        ten_sec_ago = now - timedelta(seconds=10)
        
        # Query DynamoDB for TestBucket data
        response = table.query(
            KeyConditionExpression='bucket_name = :bucket AND #ts BETWEEN :start AND :end',
            ExpressionAttributeNames={'#ts': 'timestamp'},  # 'timestamp' is reserved
            ExpressionAttributeValues={
                ':bucket': 'TestBucket',
                ':start': ten_sec_ago.isoformat(),
                ':end': now.isoformat()
            },
            ScanIndexForward=True  # Sort by timestamp ascending
        )
        
        # Get all historical data for max size calculation
        all_data = []
        last_key = None
        while True:
            if last_key:
                scan_response = table.query(
                    KeyConditionExpression='bucket_name = :bucket',
                    ExpressionAttributeValues={':bucket': 'TestBucket'},
                    ExclusiveStartKey=last_key
                )
            else:
                scan_response = table.query(
                    KeyConditionExpression='bucket_name = :bucket',
                    ExpressionAttributeValues={':bucket': 'TestBucket'}
                )
            all_data.extend(scan_response['Items'])
            last_key = scan_response.get('LastEvaluatedKey')
            if not last_key:
                break
        
        # Prepare plot data
        timestamps = [datetime.fromisoformat(item['timestamp']) for item in response['Items']]
        sizes = [item['total_size'] for item in response['Items']]
        max_size = max([item['total_size'] for item in all_data]) if all_data else 0
        
        # Generate plot
        plt.figure(figsize=(12, 6))
        ax = plt.gca()
        ax.plot(timestamps, sizes, 'b-', marker='o', label='Bucket Size')
        ax.axhline(y=max_size, color='r', linestyle='--', label=f'Max Size: {max_size} bytes')
        
        # Format plot
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.set_xlabel('Timestamp (UTC)')
        ax.set_ylabel('Size (bytes)')
        ax.set_title('TestBucket Size Changes (Last 10 seconds)')
        ax.legend()
        ax.grid(True)
        plt.tight_layout()
        
        # Save to in-memory buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        
        # Upload to S3
        s3.put_object(
            Bucket='TestBucket',
            Key='size_plot.png',
            Body=buf,
            ContentType='image/png'
        )
        plt.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Plot generated successfully',
                's3_location': 's3://TestBucket/size_plot.png'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'context': 'Failed to generate plot'
            })
        }