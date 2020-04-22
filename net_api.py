from pymongo import MongoClient
import pandas as pd 
from InsertMongoDB import * 
import re

def user_matcher(user_string):
    # Used to validate if string is an user
    user = re.match("", user_string)
    if user:
        return True
    else:
        return False

class network:
    def __init__(self, data, save_dir):
        self.data = data
        self.save_dir = save_dir
        self.json_dump = None

    def convert(self):
        frame = self.data[['screen_name','RT']]
        frame.rename(columns={"user":"source","RT":"target"}, inplace=True)
        frame.rename(columns={"screen_name":"source","RT":"target"}, inplace=True)
        frame['valid_src'] = frame.source.apply(user_matcher)
        frame['valid_target'] = frame.target.apply(user_matcher)
        valid_src_dest = frame[(frame.valid_src==True) & (frame.valid_target==True)]
        grouped_src_dst = valid_src_dest.groupby(["source","target"]).size().reset_index()

        unique_ips = pd.Index(grouped_src_dst['source']
                            .append(grouped_src_dst['target'])
                            .reset_index(drop=True).unique())

        group_dict = {}
        counter = 0
        for user_string in unique_ips:
            breakout = re.match("", user_string)
            if breakout:
                net_user = user_string
                if len(net_user) > 30:
                    net_user = net_user.split(" ")[0]
                if net_user not in group_dict:
                    counter += 1
                    group_dict[net_user] = counter
                else:
                    pass

        grouped_src_dst.rename(columns={0:'count'}, inplace=True)
        temp_links_list = list(grouped_src_dst.apply(lambda row: {"source": row['source'], "target": row['target'], "value": row['count']}, axis=1))

        links_list = []
        for link in temp_links_list:
            record = {"value":link['value'], "source":unique_ips.get_loc(link['source']),
            "target": unique_ips.get_loc(link['target'])}
            links_list.append(record)


        nodes_list = []
        for net_user in unique_ips:
            breakout = re.match("", net_user)
            if len(net_user) > 30:
                net_user = net_user.split(" ")[0]
            if breakout:
                net_id = net_user
                nodes_list.append({"name":net_user, "group": group_dict.get(net_id)})
        json_prep = {"nodes":nodes_list, "links":links_list}
        json_prep.keys()

        self.json_dump = json.dumps(json_prep, indent=1, sort_keys=True)
        print(self.json_dump)

    def save(self):
        json_out = open(self.save_dir,'w')
        json_out.write(self.json_dump)
        json_out.close()

    def send_mongoDB(self):
        insert_net(self.save_dir)
