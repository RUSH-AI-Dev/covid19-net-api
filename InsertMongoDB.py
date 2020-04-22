import json
import credentials
import pandas as pd
from pymongo import MongoClient

#pip install pymongo
#pip install pymongo[srv]
#filename name has to be string (ex. 'cmkl.json') and has to be in the same folder as this python file
def insert_net(filename):
#connection to MongoDB
    cluster = MongoClient(credentials.host)
    db = cluster[credentials.cluster]
    collection = db["graph"]
    print("Connected successfully!!!")
    #insert to MongoDB
    #Replace with own json file here krub
    #pip install pymongo
    with open(filename) as f: 
        file_data = json.load(f)
    post = file_data
    collection.delete_many({})
    collection.insert_one(post)
    print("Insert to MongoDB Successful")
    cluster.close()
