# Topogram Miner

Requirements : 

    * elasticsearch index
    * python 2.7 

Workflow :

    * receive query from kibana
    * send query to elasticsearch
    * save data to tmp CSV
    * process tmp CSV
    * save the results to MongoDB