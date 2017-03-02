import os, json
import pandas as pd
import time
from bson.objectid import ObjectId
import pymongo
import time, sys
from data_analyser import get_fields
from data_analyser import parse


def list_inbound_outbound(directory):

    list_inbound_files = []
    list_outbound_files = []

    for root, dirs, files in os.walk(directory):
        for file in files:

            if sys.platform == 'win32':
                if "inbound" in file:
                    list_inbound_files.append(''.join((root,"\\",file)))
                elif "outbound" in file:
                    list_outbound_files.append(''.join((root,"\\",file)))

            else:
                if "inbound" in file:
                    list_inbound_files.append(''.join((root,"/",file)))
                elif "outbound" in file:
                    list_outbound_files.append(''.join((root,"/",file)))

    return list_inbound_files,list_outbound_files


def retain_msisdn(msisdn):
    msisdn = str(msisdn) # from unicode
    
    if msisdn == "unknown":
        return ""

    #msisdn a regular phone number
    elif msisdn[0:3] == "+27" and len(msisdn) == 12:
        return msisdn

    #msisdn a mxit phone number
    elif msisdn[0] == 'm':
        return msisdn

    elif msisdn[0:2] == '07' or  msisdn[0:2] == '27' and len(msisdn) == 10:
        return '27'+msisdn[1:]

    elif msisdn[0:2] == '27' and len(msisdn) == 11:
        return msisdn

    ## some other type of number we don't know about

    elif msisdn[0:2] != '27' and msisdn[0:2] != '07' and  msisdn[0:3] != '+27':
        return msisdn

    

## assuming that files come in inbound-outbound pairs 
"""
this function returns a dictionary
key: is a unique participant number ("from_addr" in inbound file)
value: list of dictionaries
each dictionary is ordered by timestamp, comprising the back and forth conversation between the system and the user for a particular user 
"""
def create_conversations(inbound_file,outbound_file):

    inbound_part = {}
    outbound_part = {}


    json1 = parse(inbound_file)

    for line in json1:
        if line["from_addr"] not in inbound_part:
            inbound_part[line["from_addr"]] = [line]
        else:
            inbound_part[line["from_addr"]].append(line)


    conversations = []

    json2 = parse(outbound_file)

    for line in json2:
        if line["to_addr"] not in outbound_part:
            outbound_part[line["to_addr"]] = [line]
        else:
            outbound_part[line["to_addr"]].append(line)

    for k,v in inbound_part.items():
        if k in outbound_part:
            inbound_part[k] = inbound_part[k] + outbound_part[k]

        ##sort in order of increasing time stamp
        inbound_part[k].sort(key=lambda x:time.mktime(time.strptime(x['timestamp'], '%Y-%m-%d %H:%M:%S.%f')))
        
        for d in inbound_part[k]: 
            d["from_addr"] = retain_msisdn(d["from_addr"])
            d["to_addr"] = retain_msisdn(d["to_addr"])

    return inbound_part


"""
imports data into a dataframe of dataframes
outer level is the user and conversations array
inner level is a list of conversations in the conversation array for each user --> dicts; we could make these "official" mongodb documents if needed
"""
def import_data(inbound_list, outbound_list):
    
    conversations = {}

    if len(inbound_list) == len(outbound_list):
        for ind in range(len(inbound_list)):
            ## returns dictionary 
            ## key = from_addr category in inbound
            ## val = list of dictionaries (messages to/from system, ordered by timestamp)
            conversations.update(create_conversations(inbound_list[ind],outbound_list[ind]))

    columns = ['content',
                 'from_addr',
                 'from_addr_type',
                 'group',
                 'helper_metadata',
                 'in_reply_to',
                 'message_id',
                 'message_type',
                 'message_version',
                 'provider',
                 'routing_metadata',
                 'session_event',
                 'timestamp',
                 'to_addr',
                 'to_addr_type',
                 'transport_metadata',
                 'transport_name',
             'transport_type']

    print "Creating list of data frames...\n"
    ## transforms the values of items in conversations dictionary to be a list of pandas dataframes
    for k,v in conversations.items():
        k = k.encode('utf-8')

        tmp = []
        for d in v:
            ##label columns of messages
          #  df_conv = pd.DataFrame([d])  #each dictionary is a row in dataframe
        
            df_conv = pd.DataFrame.from_dict([d])    

            df_conv.columns = columns
            tmp.append(df_conv)
        conversations[k] = tmp


    columns = ['user', 'conversation']
    ##returns a list of top level conversation dataframes (user: conversations array)
    ## do this because cannot convert dictionaries directly to a dataframe since conversations are list of different length
    frames = []

    len_conv = len(conversations.items())
    count = 0
    print "Adding conversations to database...\n"
    for val in conversations.items():
        count+=1
        if count%10 ==0:
            print "adding %d conversations out of %d"%(count,len_conv)
        df = pd.DataFrame(([val]))
        df.columns = columns
        frames.append(df)

    ## concatenate 
    df_outer = pd.concat(frames)

    return df_outer
