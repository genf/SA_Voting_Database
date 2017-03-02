"""
solutions to small tasks requested of me
"""

import json
import os, os.path
import sys
import time

def read_json(file):

    content=[]
    with open(file,'rb') as f:
        for line in f.readlines():
            content.append(json.loads(line))
            #returns a tuple with the file name and its content, both strings
    
    return content

dir_unzipped = "C:\Users\gfried\Desktop\Research\PoliticalScience\code\data\unzipped"
root = "C:\Users\gfried\Desktop\Research\PoliticalScience\code"



group1= "+27726036721" #"+27768811752" 
group2="+27761140984" # "+27792369195"
group3= "+27618304835" #"+27728598552"

content1=""
content2=""
content3=""

output = {}

count = 0

directory =  "C:\Users\gfried\Desktop\Research\PoliticalScience\code\data\unzipped\c1pushmessage1-2014-05-06T08-32-06.203267-stopped-inbound.json"


for root, dirs, files in os.walk(dir_unzipped):
    for file in files:

        file_path = ''.join((dir_unzipped,"\\",file))
        
        if "outbound" in file:
            print "Reading file %s"%file
             
            with open(file_path, 'rb') as f:
                for line in f.readlines():
                    line = json.loads(line)

                    if line['to_addr'] == group1:
                        content1=line['content']
                        #print "Group 1 message is: %s"%content1
                       
                        if 'group1' in output:
                            output['group1'].append({"timestamp":line["timestamp"],"content":content1})
                        else:
                            output['group1'] = [{"timestamp":line["timestamp"],"content":content1}]
                    
                    if line['to_addr'] == group2:
                        content2=line['content']
                        #print "Group 2 message is: %s"%content2

                        if 'group2' in output:
                            output['group2'].append({"timestamp":line["timestamp"],"content": content2})
                        else:
                            output['group2'] = [{"timestamp":line["timestamp"],"content": content2}]
                    
                    if line['to_addr'] == group3:
                        content3=line['content']
                        #print "Group 3 message is: %s"%content3

                        if 'group3' in output:
                            output['group3'].append({"timestamp":line["timestamp"],"content": content3})
                        else:
                            output['group3'] = [{"timestamp":line["timestamp"],"content": content3}]



new_file = "C:\Users\gfried\Desktop\Research\PoliticalScience\code\groups"
fd = os.open(new_file,os.O_WRONLY|os.O_CREAT)
for k,v in output.items():

    v.sort(key=lambda x:time.mktime(time.strptime(str(x["timestamp"]), '%Y-%m-%d %H:%M:%S.%f')))
    os.write(fd,(("For %s: \n\n")%k))
    for json in v:
        print json["content"]
                        
        os.write(fd,(json["content"]+"\n"))

os.close(fd)

