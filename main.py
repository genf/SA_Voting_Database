import os, json
import pandas as pd
import datetime
import time
from bson.objectid import ObjectId
import pymongo
import time
from data_analyser import get_fields
from data_analyser import parse
from build_db import *
import pprint

directory =  'C:\\Users\\gfried\\Desktop\\Research\\PoliticalScience\\code\\data\\partial' 
inb,outb = list_inbound_outbound(directory)


conn, db, conversations_collection = mongo_database_setup()

df = import_data(inb,outb)

object_ids = mongod_insert_df(df,conversations_collection)


## populated_db is an object number...?

## list all collections on our database
print "Collection: ", db.collection_names(include_system_collections=False)



"""

for id_ in object_ids:

	doc = conversations_collection.find_one({"_id": ObjectId(id_)})


	user = doc['user']
	conversation = doc['conversation']




	# print "-------------------------------------------------------"
	# for k,v, in conversation.items():

	# 	print "------------------\n", "from_addr: %s\n"%(v["from_addr"]),"to_addr: %s\n"%(v["to_addr"]), "message: %s\n"%(v["content"]), "timestamp: %s\n"%(v["timestamp"]), "------------------\n" 


	# 	print v["timestamp"]

	# print "-------------------------------------------------------"


# ## calculate image A: transport type is either 'sms' or 'ussd'



# transport_type 

id_ = object_ids[0]
x = conversations_collection.find_one({"_id": ObjectId(id_)})
timestamp = x['conversation']['msg1']["timestamp"]
timestamp = x.split()[0]


"""