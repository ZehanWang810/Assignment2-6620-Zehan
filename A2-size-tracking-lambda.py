import boto3
import time
from datetime import datetime

# 初始化 S3 和 DynamoDB 客户端
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('S3-object-size-history')  # 使用 Part 1 创建的表名

def lambda_handler(event, context):
    # 获取存储桶名称
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    # 计算存储桶总大小和对象数量
    total_size = 0
    object_count = 0
    
    try:
        # 分页处理 S3 对象列表（处理超过 1000 个对象的情况）
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    total_size += obj['Size']
                    object_count += 1
    
    except Exception as e:
        print(f"Error calculating bucket size: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
    
    # 获取当前时间戳（UTC）
    timestamp = int(time.time())
    
    # 格式化可读时间
    current_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
    # 准备数据写入 DynamoDB
    item = {
        'bucket_name': bucket_name,
        'timestamp': timestamp,
        'total_size': total_size,
        'object_count': object_count,
        'formatted_time': current_time  # 可选：添加可读时间戳
    }
    
    # 写入 DynamoDB
    try:
        table.put_item(Item=item)
        print(f"Successfully wrote data to DynamoDB: {item}")
        return {
            'statusCode': 200,
            'body': f"Bucket {bucket_name} size calculated and stored: {total_size} bytes, {object_count} objects"
        }
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }