import json
import pandas as pd
import boto3
import os
import logging

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
destbucket = os.environ["Destination_Bucket"]
sns_arn = os.environ["SNS_ARN"]
logger = logging.getLogger()
logger.setLevel("INFO")

def lambda_handler(event, context):
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        logger.info("Source_Bucket_Name: " + bucket_name)
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        logger.info("Source_File_Key: " + s3_file_key)
        logger.info("Destination_Bucket_Name: " + destbucket)
        dest_file_key = s3_file_key.replace("raw_input", "processed-data")
        logger.info("Destination_File_Key: " + dest_file_key)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        df_s3_data = pd.read_json(resp['Body'], orient="records")
        print(df_s3_data.head())

        filtered_df = df_s3_data[df_s3_data["status"] == 'delivered']
        filtered_df.to_json('/tmp/test.json', orient="records", indent= 4)

        s3_client.put_object(Bucket= destbucket, Key= dest_file_key, Body= '/tmp/test.json')
        message = "Input S3 File {} has been processed succesfully !!".format("s3://"+ bucket_name+ "/"+ s3_file_key)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn = sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        logger.error(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+ bucket_name+ "/"+ s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')