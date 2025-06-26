import json
import pandas as pd
import boto3
from io import BytesIO

s3 = boto3.client('s3')
response = s3.get_object(Bucket='retailbotbucket', Key='export_store_cfa_ext.xlsx')

excel_data = response['Body'].read()
df = pd.read_excel(BytesIO(excel_data), engine='openpyxl')
print(df.head())
def get_info_from_db(store_id, information_type):
    # Simulate database query
    # In a real scenario, you would query your database here
    # For this example, we'll just return some dummy data
    if information_type=="CIS_flag" or information_type=="DTS_flag":
        
        filtered_df = df[(df['STORE'] == store_id) & (df['GROUP_ID'] == 20013)]
        cis_flag = filtered_df['VARCHAR2_2']
        dts_flag = filtered_df['VARCHAR2_1']
        return cis_flag, dts_flag
    elif information_type=="opening_hours" or information_type=="closing_hours":
        filtered_df = df[(df['STORE'] == store_id) & (df['GROUP_ID'] == 20013)]
        opening_hours = filtered_df['VARCHAR2_3']
        closing_hours = filtered_df['VARCHAR2_4']
        return opening_hours, closing_hours

    return dummy_data

def lambda_handler(event, context):


    try:
        print("Received event:", json.dumps(event, indent=2))
        
        slots = event['sessionState']['intent']['slots']
        store_id = slots.get('storeId', {}).get('value', {}).get('interpretedValue', 'unknown')
        information_type = slots.get('informationType', {}).get('value', {}).get('interpretedValue', 'unknown')

        if information_type == "CIS_flag" or information_type == "DTS_flag":
            cis_flag, dts_flag = get_info_from_db(store_id, information_type)
            response_message = f"Received store ID: {store_id}, info type: {information_type}, CIS_flag: {cis_flag}, DTS_flag: {dts_flag}"
        elif information_type == "opening_hours" or information_type == "closing_hours":
            opening_hours, closing_hours = get_info_from_db(store_id, information_type)
            response_message = f"Received store ID: {store_id}, info type: {information_type}, opening_hours: {opening_hours}, closing_hours: {closing_hours}"        
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": event['sessionState']['intent']['name'],
                    "state": "Fulfilled"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": response_message
                }
            ]
        }
    
    except Exception as e:
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": "GetInfoIntent",
                    "state": "Failed"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": f"Error: {str(e)}"
                }
            ]
        }
