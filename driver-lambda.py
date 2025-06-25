import json
import boto3
import time

def lambda_handler(event, context):
    """
    Driver Lambda to simulate S3 operations and trigger plotting.
    
    Operations:
    1. Create 'assignment.txt' (19 bytes)
    2. Update 'assignment.txt' (28 bytes)
    3. Delete 'assignment.txt'
    4. Create 'assignment2.txt' (2 bytes)
    5. Call plotting API
    
    Requires:
    - S3 bucket 'TestBucket'
    - plotting-lambda API endpoint configured in API_URL
    """
    
    s3 = boto3.client('s3')
    bucket_name = 'TestBucket'
    
    # Configure your plotting API endpoint here
    API_URL = 'https://xxxxxxxx.execute-api.region.amazonaws.com/prod/plot'
    
    try:
        # Operation 1: Create object
        s3.put_object(
            Bucket=bucket_name,
            Key='assignment.txt',
            Body=b'Empty Assignment 1'  # 19 bytes
        )
        time.sleep(3)  # Add delay for distinct timestamps
        
        # Operation 2: Update object
        s3.put_object(
            Bucket=bucket_name,
            Key='assignment.txt',
            Body=b'Empty Assignment 2222222222'  # 28 bytes
        )
        time.sleep(3)
        
        # Operation 3: Delete object
        s3.delete_object(
            Bucket=bucket_name,
            Key='assignment.txt'
        )
        time.sleep(3)
        
        # Operation 4: Create new object
        s3.put_object(
            Bucket=bucket_name,
            Key='assignment2.txt',
            Body=b'33'  # 2 bytes
        )
        time.sleep(3)
        
        # Operation 5: Call plotting API
        try:
            import requests
            response = requests.post(API_URL)
            api_response = response.json()
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'operations_completed': 4,
                    'plot_api_response': api_response
                })
            }
        except ImportError:
            # Fallback if requests not available
            from urllib.request import urlopen
            import urllib.error
            try:
                with urlopen(API_URL) as response:
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'operations_completed': 4,
                            'plot_api_response': 'Triggered (no requests module)'
                        })
                    }
            except urllib.error.URLError as e:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'error': str(e),
                        'message': 'Failed to call plotting API'
                    })
                }
                
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'context': 'Driver Lambda failed'
            })
        }