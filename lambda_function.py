import json
import pandas as pd
import boto3
from io import BytesIO
print('Loading function')
s3 = boto3.client('s3')
response = s3.get_object(Bucket='retailbotbucket', Key='export_store_cfa_ext.csv')

excel_data = response['Body'].read()
df = pd.read_csv(BytesIO(excel_data))
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
        
        filtered_df = df[(df['STORE'] == store_id) & (df['GROUP_ID'].isin([20016, 20017, 20018, 20019, 20020, 20021, 20022]))]
        
        day_map = {
        20016: 'Monday',
        20017: 'Tuesday',
        20018: 'Wednesday',
        20019: 'Thursday',
        20020: 'Friday',
        20021: 'Saturday',
        20022: 'Sunday'
        }

        
        result_df = filtered_df.assign(
         DAY_NAME=filtered_df['GROUP_ID'].map(day_map)
        )[['STORE', 'DAY_NAME', 'VARCHAR2_1', 'VARCHAR2_2']]

        result_df = result_df.rename(columns={'VARCHAR2_1':'OPEN_TIME', 'VARCHAR2_2': 'CLOSE_TIME' })

        # opening_hours = result_df['VARCHAR2_1']
        # closing_hours = result_df['VARCHAR2_2']
        return result_df

    #return dummy_data

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
            result_table = get_info_from_db(store_id, information_type)
            response_message = f"Received store ID: {store_id}, info type: {information_type},Result: {result_table}"        
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
