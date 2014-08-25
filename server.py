import os
import zerorpc
import logging
from es_save_results import es2csv, es2mongo
from analyze_meme import analyze_meme
from bson.objectid import ObjectId

logging.basicConfig()

# write path and files
if "TOPOGRAM_TMP_PATH" in os.environ:
    tmp_path=os.environ.get('TOPOGRAM_TMP_PATH')
else: tmp_path='/tmp'

class HelloRPC(object):

    def es2mongo(self, data):
        collection="memes"
        data["_id"]
        print data
        meme_id=ObjectId(data["_id"])
        es2mongo(meme_id,collection)

        return { 
            "status": "done"
        }

    def es2csv(self, data):
        
        # Setup variables
        indexes_names=[data["index"]] 
        meme_name=data["name"] # TODO : change to  id
        meme_keywords=[data["term"]] # TODO : parse query

        # path
        results_path=os.path.join(tmp_path,meme_name)

        if not os.path.exists(results_path):
            os.makedirs(results_path)
        csv_file=os.path.join(results_path,meme_name+".csv")
        log_file=os.path.join(results_path,meme_name+".log")

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

        # print(meme_name, query, indexes_names)
        print "query es"
        es2csv(meme_name, data["term"], indexes_names, csv_file, log_file)

        with open (log_file, "r") as logfile:
            log=logfile.read()

        return { 
            "status": "done",
            "csv_file_path" : "csv_file",
            "log" : log,
            "query":data
        }

    def processData(self, data):
        collection="memes"
        data["_id"]
        print data
        meme_id=ObjectId(data["_id"])

        analyze_meme(meme_id,collection)
        # es2mongo(meme_id,collection)

        return { 
            "status": "done"
        }

s = zerorpc.Server(HelloRPC())
s.bind("tcp://0.0.0.0:4242")
s.run()