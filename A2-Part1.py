import boto3
from botocore.exceptions import ClientError

# AWS Region (modify as needed, e.g., 'us-east-1')
AWS_REGION = "us-east-1"

# Resource names (S3 bucket must be globally unique!)
S3_BUCKET_NAME = "testbucket-unique-name-2025"  # Replace with your unique bucket name
DDB_TABLE_NAME = "S3-object-size-history"

def create_s3_bucket(bucket_name, region=AWS_REGION):
    """
    Create an S3 bucket with the specified name.
    
    Args:
        bucket_name (str): The name of the S3 bucket (must be globally unique).
        region (str): The AWS region to create the bucket in.
        
    Returns:
        bool: True if the bucket was created successfully or already exists, False otherwise.
    """
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        # Check if the bucket already exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[INFO] S3 bucket {bucket_name} already exists")
        return True
    except ClientError as e:
        # Error code 404 indicates the bucket does not exist
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            # Non-us-east-1 regions require LocationConstraint
            if region != "us-east-1":
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            else:
                s3_client.create_bucket(Bucket=bucket_name)
            print(f"[SUCCESS] S3 bucket {bucket_name} created successfully")
            return True
        else:
            print(f"[ERROR] Failed to create S3 bucket: {e}")
            return False

def create_dynamodb_table(table_name, region=AWS_REGION):
    """
    Create a DynamoDB table to store S3 bucket size history.
    
    Args:
        table_name (str): The name of the DynamoDB table.
        region (str): The AWS region to create the table in.
        
    Returns:
        bool: True if the table was created successfully or already exists, False otherwise.
    """
    dynamodb = boto3.resource('dynamodb', region_name=region)
    
    try:
        # Check if the table already exists
        table = dynamodb.Table(table_name)
        table.load()  # Attempt to load table metadata
        print(f"[INFO] DynamoDB table {table_name} already exists")
        return True
    except ClientError as e:
        # Error code ResourceNotFoundException indicates the table does not exist
        if "ResourceNotFoundException" in str(e):
            # Define table schema:
            # - Partition Key (HASH): bucket_name (string)
            # - Sort Key (RANGE): timestamp (number)
            # This allows grouping by bucket and sorting by time
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'bucket_name', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'bucket_name', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'}
                ],
                BillingMode='PAY_PER_REQUEST'  # Pay-per-request billing mode
            )
            # Wait for table creation to complete
            table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
            print(f"[SUCCESS] DynamoDB table {table_name} created successfully")
            return True
        else:
            print(f"[ERROR] Failed to create DynamoDB table: {e}")
            return False

if __name__ == "__main__":
    # 1. Create S3 bucket
    create_s3_bucket(S3_BUCKET_NAME, AWS_REGION)
    
    # 2. Create DynamoDB table
    create_dynamodb_table(DDB_TABLE_NAME, AWS_REGION)