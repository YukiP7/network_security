from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os , sys
import pandas as pd
from json import loads as json_loads
import certifi

ca = certifi.where()

load_dotenv()
username = os.getenv('DB_USER_NAME')
password = quote_plus(os.getenv('DB_PASSWORD'))

MONGODB_URL = f"mongodb+srv://{username}:{password}@cluster0.h2daahm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# # Create a new client and connect to the server
# client = MongoClient(uri)

# # Send a ping to confirm a successful connection
# try:
#     client.admin.command('ping')
#     print("Pinged your deployment. You successfully connected to MongoDB!")
# except Exception as e:
#     print(e)

from networksecurity.logging.logger import logging
from networksecurity.exception.exception import NetworkSecurityException

class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def cv_to_json(self , file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True , inplace=True)
            records = list(json_loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    
    def push_data_to_mongoDB(self, records , database , collection):
        try:
            self.records = records
            self.database = database 
            self.collection = collection

            self.mongo_client = MongoClient(MONGODB_URL , tlsCAFile=ca)
            self.database = self.mongo_client[self.database]
            self.collection = self.database[self.collection]
            self.collection.insert_many(self.records)
            logging.info(f"Records inserted in MongoDB database : {database} and collection : {collection}")
            return (len(self.records))
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
if __name__ == "__main__":
    FILE_PATH = "Network_Data\phisingData.csv"
    DATABASE = "YUKTI"
    COLLECTION = "NetworkData"
    networkobj = NetworkDataExtract()
    records = networkobj.cv_to_json(file_path=FILE_PATH)
    print(records)
    no_of_records = networkobj.push_data_to_mongoDB(records , DATABASE , COLLECTION)
    print(f"No of records inserted in MongoDB : {no_of_records}")