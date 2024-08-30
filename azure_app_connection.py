import azure.functions as func
import logging
import mysql.connector
from mysql.connector import Error
import pandas as pd
from pandas import json_normalize
import json
from jsonschema.exceptions import ValidationError
from jsonschema import validate
from utilities import get_schema
from datetime import datetime


app = func.FunctionApp()

schema_content = None

def fetch_schema_once():

    global schema_content  
    if schema_content is None: 
        try:
            logging.info("Fetching schema...")
            schema_content = get_schema() 
            logging.info("Schema fetched and stored successfully.")
        except Exception as e:
            logging.error(f"Failed to retrieve schema: {e}")
            schema_content = None 
    else:
        logging.info("Using cached schema.")


fetch_schema_once()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="evh-commondataplatform-dev-eastus-hyla-dev",
                               connection="Connectionstringeventhub")


def eventhub_trigger2(azeventhub: func.EventHubEvent):
    global schema_content 

    # Check if schema is available
    if schema_content is None:
        logging.error("Schema is not available; cannot process the event.")

    # Deserialize the incoming JSON data
    json_data = json.loads(azeventhub.get_body().decode('utf-8'))
    logging.info(type(json_data))
    
    
    try:
        validate(instance=json_data, schema=schema_content)
        logging.info("JSON data is valid.")
        
        # If valid, insert the data into the MySQL table
        inserting_to_mysql(json_data)

    except ValidationError as err:
        logging.error("JSON data is invalid.")
        logging.error(err.message)


def inserting_to_mysql(event_data):
    try:
        mydb = mysql.connector.connect(
            host="opstech-mysql-dev.mysql.database.azure.com",
            user="opstech",
            password="hyla4ever!",
            database="Mydatabase"
        )

        if mydb.is_connected():
            logging.info("Successfully connected to the database")
            cursor = mydb.cursor()

            correlationId = event_data['correlationId']
            messageId = event_data['messageId']
            messageId = event_data['messageId']


            df = json_normalize(
                event_data['data'][0], 
                'items', 
                ['orderId', 'userId', 'orderDate', 'totalAmount', 'currency', ['shippingAddress', 'street'], ['shippingAddress', 'city'], ['shippingAddress', 'state'], ['shippingAddress', 'zipCode'], ['shippingAddress', 'country']]
            )
            df['correlation_id'] = correlationId
            df['message_id'] = messageId


            df = df.astype({
                'productId': 'str',
                'productName': 'str',
                'quantity': 'int',
                'price': 'float',
                'orderId': 'str',
                'userId': 'str',
                'orderDate': 'str',
                'totalAmount': 'float',
                'currency': 'str',
                'shippingAddress.street': 'str',
                'shippingAddress.city': 'str',
                'shippingAddress.state': 'str',
                'shippingAddress.zipCode': 'int',
                'shippingAddress.country': 'str',
                'correlation_id': 'str', 
                'message_id': 'str',
                })

            # Rename columns to match MySQL table structure
            df.columns = df.columns.str.replace('.', '_')
            # df.rename(columns={
            #     'shippingAddress.street': 'shippingAddress_street',
            #     'shippingAddress.city': 'shippingAddress_city',
            #     'shippingAddress.state': 'shippingAddress_state',
            #     'shippingAddress.zipCode': 'shippingAddress_zipCode',
            #     'shippingAddress.country': 'shippingAddress_country'
            # }, inplace=True)  
            
            for index, row in df.iterrows():
                sql = """
                    INSERT INTO airflow.demodata (
                        productId, productName, quantity, price, 
                        orderId, userId, orderDate, totalAmount, 
                        currency, shippingAddress_street, shippingAddress_city, 
                        shippingAddress_state, shippingAddress_zipCode, shippingAddress_country,
                        correlation_id, message_id, inserted_at, exported_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    row['productId'], row['productName'], row['quantity'], row['price'],
                    row['orderId'], row['userId'], row['orderDate'], row['totalAmount'],
                    row['currency'], row['shippingAddress_street'], row['shippingAddress_city'],
                    row['shippingAddress_state'], row['shippingAddress_zipCode'], row['shippingAddress_country'],
                    correlationId, messageId , datetime.now(), '9999-12-31'
                )

                # Execute the SQL command
                cursor.execute(sql, values)

            mydb.commit()
            logging.info("Data inserted successfully")

            cursor.close()

    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")

    finally:
        if mydb.is_connected():
            mydb.close()
            logging.info("MySQL connection closed")
