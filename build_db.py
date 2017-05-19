import os, json
import pandas as pd
import time
from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
import time
from data_analyser import get_fields
from data_analyser import parse
from structuredata import *

##############################################
##############################################
#### setting up mongodb database for data ####
##############################################
##############################################

def mongo_database_setup():
    database_name={}

    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn = MongoClient()
        print "Connected successfully to MongoDB, instance created!"
        
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e 

    name='SA_Voting_Data'

    ## create database on instance
    if name in conn.database_names():
        conn.drop_database(name) #Drop the database if it exists
        db = conn[name] #Create the database

        ## create one collection, called conversations
        collection = db.conversations_collection


    else:
        db = conn.name #create the database
        collection = db.conversations_collection

    ## collection is a collection in the mongodb instance created above
    ## db is the database on the mongodb instance
    ## conn is the mongodb instance
    return conn, db, collection



## This function takes a pandas dataframe and inserts it into local mogodb
## df : pandas dataframe 
## conversations_db : database we want to insert data into
def mongod_insert_df(df, conversations_collection):
    len_df = len(df)
    inserts = []

    for row in df.iterrows():
        new_dict = {}
        conv_dict = {}

        
        ## creates a single document for each message in a conversation array
        conv = row[1]['conversation']
        ind = 1
        for msg in conv:


            ## some funky stuff I found on the internet on how to insert a pandas dataframe into a mongodb using pymongo;
            ## caveat is that types may not be preserved. as of this stage (after conversations have been created) 
            ## I dont think it matters... if that turns out to be false, I will have to manually convert types, or find another solution -- 
            ## possibly using odo. source: http://tinyurl.com/jp83aoe
            conv_dict['msg%d'%ind] = (json.loads(msg.T.to_json()).values())[0] ## this produces an array with one value for some reason, so we just take this value --> makes indexing later easier
            #conv_dict['msg%d'%ind] = msg
            ind+=1

        new_dict['user'] = row[1]['user']
        new_dict['conversation'] = conv_dict

        #ready to insert to mongodb
        
        ## list of ids for each document 
        inserts.append(conversations_collection.insert_one(new_dict).inserted_id)
    

        ## inserts is an array of object id for documents in conversation collection
   # print 'There was a total of {} out of {}'.format(len(inserts),length_of_df) 
    return inserts


