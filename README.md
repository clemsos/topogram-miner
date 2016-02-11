
> This project has moved to https://github.com/topogram/topogram 
> Keeping this as an archive

# Topogram Miner

``Topogram Miner`` is a toolset to select and analyze large amounts of tweets. 

## How it works

Start the Python server

    python server.py

The miner is now ready to receive information from the UI (see [topogram-ui](https://github.com/topogram/topogram-ui) )

## Spec

**Solutions**

* Indexing : elasticsearch.  
* Data mining : python
* Storage fo result : MongoDB
* Communication with Node server : RPC Python

**Features**

* plain-text search engine (elasticsearch)
* conversational grapĥ (RT, @, comment)
* NLP and semantic analysis (support English and Chinese Language)
* user geo-localisation from profile information
* network analysis (measures, etc.)
* sync with node for asynchronous mining tasks (RPC)


### Requirements

    * elasticsearch index
    * python 2.7 
    * mongoDB
