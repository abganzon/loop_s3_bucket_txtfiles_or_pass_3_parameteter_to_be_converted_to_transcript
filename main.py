import json
import boto3
import re
import requests
import os
import snowflake.connector
s3 = boto3.client('s3')

def lambda_handler(event, context):
    
     # Set the S3 bucket name and region
    bucket_name = 'gbl-comprehend-retreaver-whisper'
    
     # Create an S3 client
    s3 = boto3.client('s3')

    # List all the objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Loop through the first 5 text files in the bucket
    count = 0
    for obj in response['Contents']:
        # Check if the object is a text file
        if obj['Key'].endswith('.txt'):
            # Retrieve the content and name of the file
            response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            file_name = obj['Key']
            print(file_name)
            
            file_parts = file_name.split('-')
            date = file_parts[0] + '-' + file_parts[1] + '-' + file_parts[2]
            call_id_parts = '-'.join(file_parts[3:11])
            call_id = call_id_parts[:-4]
            # print(f"Date: {date}")
            # print(f"Call ID: {call_id}")
            
            entities = extract_entities_from_s3('gbl-comprehend-retreaver-whisper', file_name)
            json_data = json.dumps(entities, indent=4)
            # print(entities)
            
            conn = snowflake.connector.connect(
                user=os.environ['SNOWFLAKE_USER'],
                password=os.environ['SNOWFLAKE_PASSWORD'],
                account=os.environ['SNOWFLAKE_ACCOUNT'],
                warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
                database=os.environ['SNOWFLAKE_DATABASE'],
                schema=os.environ['SNOWFLAKE_SCHEMA']
            )
            cur = conn.cursor()
            
            sql_query = "SELECT RECORDINGURL FROM CL_RETREAVER WHERE CALLUUID = %s"
            cur.execute(sql_query, (call_id,))
            results = cur.fetchall()
            
            audio_url = None
            if results:
                audio_url = results[0][0].replace("'", "")

            # print(value)
           
            sql_query1 = "INSERT INTO WHISPER_TO_COMPREHEND (TIMESTAMP, CALL_ID, AUDIO_URL, DATA) VALUES (%s, %s, %s, %s)"
            sql = cur.execute(sql_query1, (date, call_id, audio_url, json_data))
            
            conn.commit()
            cur.close()
            conn.close()

            # count += 1
            # if count == 5:
            #     break
            
            
    #if mag test manual ug select sa s3 bucket 
    # entities = extract_entities_from_s3('gbl-comprehend-retreaver-whisper', '2023-02-22-9d17a902-0358-461c-b293-72f56bc5b4eb.txt')
    # return entities
    
#     #Set the Snowflake connection parameters
#     conn = snowflake.connector.connect(
#         user=os.environ['SNOWFLAKE_USER'],
#         password=os.environ['SNOWFLAKE_PASSWORD'],
#         account=os.environ['SNOWFLAKE_ACCOUNT'],
#         warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
#         database=os.environ['SNOWFLAKE_DATABASE'],
#         schema=os.environ['SNOWFLAKE_SCHEMA']
#     )
    
#     # Create a cursor object to execute SQL commands
#     cur = conn.cursor()
    
#     url = "https://transcribe.whisperapi.com"
#     timestamp = event['queryStringParameters']['timestamp']
#     call_id = event['queryStringParameters']['call_id']
#     audio_url = event['queryStringParameters']['audio_url']

#     headers = {
#         'Authorization': 'Bearer '+ os.getenv('WHISPER_API')
#     }
#     data = {
#         "fileType": "YOUR_FILE_TYPE",  # default is wav
#         "diarization": "false",
#         "numSpeakers": "2",
#         "url": audio_url,  # can't have both a url and file sent!
#         "language": "en", 
#         "task": "transcribe"
#     }
    
#     response = requests.post(url, headers=headers, data=data)
    
#     result = extract_entities(response.text)
    
#     json_data = json.dumps(result, indent=4)

#     sql_query = "INSERT INTO WHISPER_TO_COMPREHEND (TIMESTAMP, CALL_ID, AUDIO_URL, DATA) VALUES (%s, %s, %s, %s)"
#     sql = cur.execute(sql_query, (timestamp, call_id, audio_url, json_data))
   
#     conn.commit()
#     cur.close()
#     conn.close()
    
#     #Define the S3 bucket and file name
#     bucket_name = 'gbl-comprehend-retreaver-whisper'
#     file_name = timestamp+'-'+call_id+'.txt'
    
#     # Upload the data to S3
#     s3.put_object(Bucket=bucket_name, Key=file_name, Body=response.text)
    
#     return {
#         'statusCode': 200,
#         'body': 'Data saved successfully in Snowflake table and S3 bucket'
#     }
    

def extract_entities_from_s3(bucket_name, object_key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(object_key)
    text = obj.get()['Body'].read().decode('utf-8')

    return extract_entities(text)

def extract_entities(input_text):
    comprehend = boto3.client('comprehend')
    response = comprehend.detect_entities(Text=input_text, LanguageCode='en')
    entities = response['Entities']
    
    #return entities
    result = {}

    for entity in entities:
        if entity['Type'] == 'PERSON':
            if 'Name' in result:
                if entity['Text'] not in result['Name']:
                    result['Name'] += ' ' + entity['Text']
            else:
                result['Name'] = entity['Text']
        elif entity['Type'] == 'ORGANIZATION':
            if 'Organization' in result:
                if entity['Text'] not in result['Organization']:
                    result['Organization'] += ' ' + entity['Text']
            else:
                result['Organization'] = entity['Text']

        if entity['Type'] == 'ADDRESS':
            if 'Address' in result:
                if entity['Text'] not in result['Address']:
                    result['Address'] += ' ' + entity['Text']
            else:
                result['Address'] = entity['Text']
        elif entity['Type'] == 'LOCATION':
            if 'Location' in result:
                if entity['Text'] not in result['Location']:
                    result['Location'] += ' ' + entity['Text']
            else:
                result['Location'] = entity['Text']

        elif entity['Type'] == 'PHONE_NUMBER' or entity['Type'] == 'OTHER':
            number_text = entity['Text']
            if len(number_text) == 5 and "." not in number_text and not any(entity['Type'] == 'ZIP_CODE' for entity in entities) and number_text.isdigit():
                result['ZipCode'] = int(number_text)
                
            elif isinstance(number_text, int) or (isinstance(number_text, str) and number_text.isdigit() and '.' not in number_text) and len(number_text) == 10 and not any(entity['Type'] == 'PHONE_NUMBER' for entity in entities)and number_text.isdigit():
              if 'Priority Number' in result:
                    if number_text not in result['Priority Number']:
                        result['Priority Number'] += ' ' + str(number_text)
              else:
                    result['Priority Number'] = str(number_text)

    return result
