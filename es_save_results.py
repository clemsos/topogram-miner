#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unicodedata
import csv
import os
import elasticsearch
from lib.mongo import MongoDB
from time import time

# elasticsearch
if "TOPOGRAM_ES_HOST" in os.environ:
    es_host=os.environ.get('TOPOGRAM_ES_HOST')
else: es_host='localhost:9200'

es = elasticsearch.Elasticsearch([es_host])

# Connect to Mongo
db=MongoDB("weibodata").db


# Chunk size to write CSV rows in file
chunksize=1000

def es2mongo(post_id, collection):
    t0=time()
    memes = db[collection]
    meme=memes.find_one({"_id": post_id})

    print meme 

    query=meme["term"]
    try:
        indexes_names=[meme["index"]]
    except KeyError:
        indexes_names=[]

    for i,index_name in enumerate(indexes_names):
            
        # Get the number of results
        res = es.search(index=index_name, q=query)    
        data_size=res['hits']['total']
        print "Total %d Hits from %s" % (data_size, index_name)

        if data_size==0:continue #avoid empty results

        # Get numbers of results 
        for chunk in xrange(0,data_size,chunksize):
            
            # display progress as percent
            per=round(float(chunk)/data_size*100, 1)

            # request data
            res=es.search(index=index_name, q=query, size=chunksize, from_=chunk)

            print "%.01f %% %d Hits Retreived - fiability %.3f" % (per,chunk, res['hits']['hits'][0]["_score"])
            
            if res['hits']['hits'][0]["_score"] < 0.2 : break


            # get headers
            if i==0 : headers=[value for value in res['hits']['hits'][0]["_source"]]

            for sample in res['hits']['hits']:
                row={}
                for id in sample["_source"]:
                    if type(sample["_source"][id]) == unicode : data = sample["_source"][id].encode("utf-8") 
                    else : data = sample["_source"][id] 
                    row[id]=data

                # store in Mongo
                memes.update({"_id": post_id}, {'$addToSet': {'messages':row }})

    #         # Write log file
    #         with open(log_file, 'ab') as logfile: 
    #             logfile.write("index_name : %s \n"%index_name)
    #             logfile.write("meme_name : %s \n"% meme_name)
    #             logfile.write("query : %s \n"% query)
    #             logfile.write("%.01f %% %d Hits Retreived - fiability %.3f \n" % (per,chunk, res['hits']['hits'][0]["_score"]) )
    #             logfile.write("Done. Data saved in %s \n"%csv_file)

    
    print "Done in %.3fs. Data stored in MongoDB"%(time()-t0)

def es2csv(meme_name, query, indexes_names, csv_file, log_file):

    # Open a csv file and write the stuff inside
    with open(csv_file, 'wb') as csvfile: 

        filewriter = csv.writer(csvfile)

        for i,index_name in enumerate(indexes_names):
            
            # Get the number of results
            res = es.search(index=index_name, q=query)
            data_size=res['hits']['total']
            print "Total %d Hits from %s" % (data_size, index_name)

            if data_size==0:continue #avoid empty results

            # file headers
            if i==0 : 
                # get headers
                headers=[value for value in res['hits']['hits'][0]["_source"]]

                # create column header row
                filewriter.writerow(headers)

            # Get numbers of results 
            for chunk in xrange(0,data_size,chunksize):
                
                # display progress as percent
                per=round(float(chunk)/data_size*100, 1)

                # request data
                res=es.search(index=index_name, q=query, size=chunksize, from_=chunk)

                print"%.01f %% %d Hits Retreived - fiability %.3f" % (per,chunk, res['hits']['hits'][0]["_score"])
                # if res['hits']['hits'][0]["_score"] < 0.2 : break

                for sample in res['hits']['hits']: 
                    row=[]
                    for id in sample["_source"]:
                        if type(sample["_source"][id]) == unicode : data = sample["_source"][id].encode("utf-8") 
                        else : data = sample["_source"][id] 
                        row.append(data)

                    filewriter.writerow(row)

            # Write log file
            with open(log_file, 'ab') as logfile: 
                logfile.write("index_name : %s \n"%index_name)
                logfile.write("meme_name : %s \n"% meme_name)
                logfile.write("query : %s \n"% query)
                logfile.write("%.01f %% %d Hits Retreived - fiability %.3f \n" % (per,chunk, res['hits']['hits'][0]["_score"]) )
                logfile.write("Done. Data saved in %s \n"%csv_file)

    print "Done. Data saved in %s"%csv_file
    print "Log is stored at %s"%log_file