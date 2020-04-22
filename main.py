#%%
from pymongo import MongoClient
from net_api import *
import pandas as pd 
import credentials

#%%
if __name__ == "__main__":
    con = MongoClient(credentials.host)
    db = con.get_database(credentials.cluster)
    obj = db.TwitterNews
    data = pd.DataFrame(list(obj.find())) 
    net = network(data, 'cmkl.json')
    net.convert()
    net.save()
    net.send_mongoDB()


# %%
