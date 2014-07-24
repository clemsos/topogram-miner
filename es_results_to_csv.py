#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unicodedata
import csv
import os
import elasticsearch

es = elasticsearch.Elasticsearch(["localhost:9200"])

# Setup your variables
indexes_names=[
    "weiboscope_39_40",
    # "weiboscope_41_42",
    # "weiboscope_43_44",
    # "weiboscope_45_46",
    #"weiboscope_47_48",
    #"weiboscope",
    #"weiboscope_49_50"
    ]
meme_name="ls58"
meme_keywords=["未来"]


results_path="/home/clemsos/Dev/mitras/results/"+meme_name

# write path and files
if not os.path.exists(results_path):
    os.makedirs(results_path)
csv_file=results_path+"/"+meme_name+".csv"
log_file=results_path+"/"+meme_name+".log"


# ES : config
chunksize=1000

# ES : Build query
meme_query=""
for i,k in enumerate(meme_keywords):
    meme_query+='\"'+k+'\"'
    if i+1 < len(meme_keywords): meme_query+= " OR "


query={ "query": {
        "query_string": {
            "query": meme_query
         }
      }
    }

# Open a csv file and write the stuff inside
with open(csv_file, 'wb') as csvfile: 

    filewriter = csv.writer(csvfile)

    for i,index_name in enumerate(indexes_names):
        
        # Get the number of results
        res = es.search(index=index_name, body=query)
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
            res=es.search(index=index_name, body=query, size=chunksize, from_=chunk)

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
            logfile.write("meme_query : %s \n"% meme_query)
            logfile.write("%.01f %% %d Hits Retreived - fiability %.3f \n" % (per,chunk, res['hits']['hits'][0]["_score"]) )
            logfile.write("Done. Data saved in %s \n"%csv_file)


print "Done. Data saved in %s"%csv_file
print "Log is stored at %s"%log_file

