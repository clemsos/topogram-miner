#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, csv, json
from time import time 
import datetime
from collections import Counter
import networkx as nx
import community
import locale

import lib.tweetminer as minetweet
from lib.users import UserAPI
from lib.nlp import NLPMiner
from lib.mongo import MongoDB

# 
if "TOPOGRAM_TMP_PATH" in os.environ:
    tmp_path=os.environ.get('TOPOGRAM_TMP_PATH')
else: tmp_path='/tmp'

results_path=tmp_path

# Connect to Mongo
collection="memes"
db=MongoDB("weibodata").db


# meme_names=[ meme for meme in os.listdir(results_path) if meme[-3:] != "csv"]
# meme_names=[
 # 'biaoge',
 # 'thevoice',
 # 'hougong',
 # 'gangnam',
 # 'sextape',
 # 'dufu',
 # 'moyan',
 # 'ccp',
 # 'yuanfang',
 # 'qiegao']

meme_names=[
 # 'tuhao',
 # 'diaosi'
 "ls58"
]
# meme_names=['biaoge']
print meme_names

t0=time()
minetweet.init_tweet_regex()

locale.setlocale(locale.LC_ALL, "")

nlp=NLPMiner()

stoplist=[i.strip() for i in open("lib/stopwords/zh-stopwords","r")]
stoplist+=[i.strip() for i in open("lib/stopwords/stopwords.txt","r")]
stoplist+=["转发","微博","说 ","一个","【 ","年 ","转 ","请","＂ ","问题","知道","中 ","已经","现在","说","【",'＂',"年","中","今天","应该","真的","月","希望","想","日","这是","太","转","支持"]
# stoplist+=["事儿","中国"]

api=UserAPI()
words_users_time=[]

def get_province(_userid):
    province_code= api.get_province(_userid)
    # print province_code
    try :
        return api.provinces[province_code]
    except KeyError :
        return 0

for meme_name in meme_names:

    # Init
    # tstart=time()
    print "Processing meme '%s'"%meme_name

    # files names
    meme_path=outfile=os.path.join(results_path,meme_name)
    meme_csv=os.path.join(meme_path,meme_name)+".csv"

    jsondata={}
    jsondata["meme_name"]=meme_name

    users=[]
    users_edges=[]

    words=[]
    words_users=[]
    words_edges={}
    tweets_count=0

    by_time={}
    print "processing tweets..."
    
    # process the data
    with open(meme_csv, 'rb') as csvfile:
        memecsv=csv.reader(csvfile)
        memecsv.next() # skip headers

        for row in memecsv:
            # extract text
            t=row[1]    
            tweets_count+=1
            
            # time (round and store)
            d=datetime.datetime.strptime(row[9], "%Y-%m-%dT%H:%M:%S")
            day = datetime.datetime(d.year,d.month,d.day,d.hour,0,0) # round to hour
            # day = datetime.datetime(d.year,d.month,d.day,0,0,0) # round to day
            timestamp=day.strftime("%s")
            
            # regexp extract tweet entities
            mentions,urls,hashtags,clean=minetweet.extract_tweet_entities(t)
                      
            # User diffusion graph
            user_diff=[]
            users_to_users=[]
            for mention in mentions:
                users_to_users.append((row[0],mention))
                # user_edges_time.append((row[0],mention,timestamp))
                if mention not in user_diff : user_diff.append(mention)

                # retweeted_uid
            if row[7] != "" : 
                users_to_users.append((row[7],row[0]))
                # user_edges_time.append((row[7],row[0],timestamp))
                if row[7] not in user_diff : user_diff.append(row[7])
            
            users_edges+=users_to_users # store all users interactions
            users+=user_diff # store all users
            
            # extract text 
            dico=nlp.extract_dictionary(clean)

            # remove stopwords and get clean dico
            clean_dico=nlp.remove_stopwords(dico)
            
            # remove more stopwords
            tmp_words=[
                    w for w in clean_dico 
                    if w.encode('utf-8') not in stoplist 
                    and w[0] != "u" ]

            words+=tmp_words # store all words
            
            # words edges
            words_to_words=[]
            words_to_users=[]
            for w in tmp_words :
                
                # word edges
                words_to_words+=[(w,t) for t in tmp_words if t!=w]
                
                # word to users
                words_to_users+=[(w,u) for u in user_diff]
                
                try: words_edges[w]
                except KeyError: words_edges[w]=[]
                words_edges[w]+=[t for t in tmp_words if t!=w]
            
            # words_edges+=words_to_words
            words_users+=words_to_users
            # words_edges+=words_to_words # store all interactions
            
            # store data by time
            try : by_time[timestamp]
            except KeyError: by_time[timestamp]={}
                
            # count
            try : by_time[timestamp]["count"]
            except KeyError: by_time[timestamp]["count"]=0
            by_time[timestamp]["count"]+=1
            
            # users edges
            try: by_time[timestamp]["user_edges"]
            except KeyError: by_time[timestamp]["user_edges"]=[]
            by_time[timestamp]["user_edges"]+=users_to_users
            
            # users nodes
            try: by_time[timestamp]["user_nodes"]
            except KeyError: by_time[timestamp]["user_nodes"]=[]
            by_time[timestamp]["user_nodes"]+=user_diff
            
            # words nodes
            try: by_time[timestamp]["words_nodes"]
            except KeyError: by_time[timestamp]["words_nodes"]=[]
            by_time[timestamp]["words_nodes"]+=tmp_words
            
            # word edges
            try: by_time[timestamp]["words_edges"]
            except KeyError: by_time[timestamp]["words_edges"]=[]
            by_time[timestamp]["words_edges"]+=words_to_words
            
            # word edges
            try: by_time[timestamp]["words_to_users"]
            except KeyError: by_time[timestamp]["words_to_users"]=[]
            by_time[timestamp]["words_to_users"]+=words_to_users

    print "processing done"
    print "%d tweets"%tweets_count
    print "%d timeframes "%len(by_time)

    print "USERS"
    print "-"*10

    top_users_limit=500 # only 500 top users
    users_edges_limit=500

    # limit to 500 users
    top_users=[c[0] for c in Counter(users).most_common(top_users_limit)]
    print "%d top_users"%len(top_users)

    # only edges that contains top users
    top_users_edges=[e for e in users_edges if e[0] in top_users and e[1] in top_users ]
    print "%d top_users_edges"%len(top_users_edges)

    print "Parsing user provinces"
    print "-"*10
    # parse provinces for all users
    user_provinces={}
    provinces_stats={}

    for user in top_users:
        province=get_province(user)
        user_provinces[user]=province
        try : provinces_stats[province]
        except KeyError: provinces_stats[province]=0
        provinces_stats[province]+=1

    # stats by province
    users_by_provinces_stats=[{
                                "label": p,
                                "count":provinces_stats[p]
                                } for p in provinces_stats]

    # User graph info
    print "USER GRAPH"
    print "-"*10
    print "Edges (total number) : %d edges"%len(users_edges)

    # define acceptable minimum value
    users_minimum_exchange=0
    users_edges_count=[p[1] for p in Counter(top_users_edges).most_common()]

    i=0
    for x in reversed(Counter(users_edges_count).most_common()):
        i+=x[1]
        users_minimum_exchange=x[0]
        if i> users_edges_limit: break
    print "users_minimum_exchange: %d"%users_minimum_exchange
    
    # Weighted users edges that have a minimum value of minimum_exchange
    edges_weighted=[
            str(p[0][0]+" "+p[0][1]+" "+str(p[1])) 
            for p in Counter(top_users_edges).most_common(users_edges_limit) 
            if p[1] > users_minimum_exchange] 

    print "Weighted edges %d"%len(edges_weighted)

    # create graph object
    G = nx.read_weighted_edgelist(edges_weighted, nodetype=str, delimiter=" ",create_using=nx.DiGraph())

    # dimensions
    N,K = G.order(), G.size()
    print "Nodes: ", N
    print "Edges: ", K

    allowed_users=G.nodes()
    print "%d allowed_users"%len(allowed_users)

    # Average degree
    # avg_deg = float(K)/N
    # print "Average degree: ", avg_deg

    # Average clustering coefficient
    # ccs = nx.clustering(G.to_undirected())
    # avg_clust_coef = sum(ccs.values()) / len(ccs) 
    # print "Average clustering coeficient: %f"%avg_clust_coef
        
    # Communities
    user_communities = community.best_partition(G.to_undirected()) 
    print "Number of partitions : ", len(set(user_communities.values()))
    # modularity=community.modularity(user_communities,G.to_undirected())
    # print "Modularity of the best partition: %f"%modularity

    # betweeness_centrality
    # print "computing betweeness_centrality... (this may take some time)"
    # # users_btw_cent=nx.betweenness_centrality (G.to_undirected())
    # print "computing done"

    
    # PROVINCES graph
    print 
    print "PROVINCES GRAPH"
    print "-"*20
    province_edges_weighted=[]
    for edge in edges_weighted:
        e=edge.split()
        try : s=user_provinces[e[0]]
        except KeyError: pass
        try : t=user_provinces[e[1]]
        except KeyError: pass 
        if s!=0 and t!=0: province_edges_weighted.append((str(s).replace(" ","_")+" "+str(t).replace(" ","_")+" "+str(e[2])))

    Gp = nx.read_weighted_edgelist(province_edges_weighted, nodetype=str, delimiter=" ",create_using=nx.DiGraph())

    # dimensions
    Np,Kp = Gp.order(), Gp.size()
    print "Nodes: ", Np
    print "Edges: ", Kp

    # Communities
    provinces_communities = community.best_partition(Gp.to_undirected())
    print "Number of partitions : ", len(set(provinces_communities.values()))
    # modularity=community.modularity(user_communities,Gp.to_undirected())
    # print "Modularity of the best partition: %f"%modularity

    # WORD graph info
    print 
    print "WORD GRAPH"
    print "-"*20
    print "%d words edges"%len(words_edges)

    # words_minimum_exchange
    top_words_limit=500
    words_edges_limit=350

    top_words=[c[0] for c in Counter(words).most_common(top_words_limit)]
    print "%d top_words"%len(top_words)

    top_words_edges=[]
    for word in words_edges:
        if word in top_words: 
            for c in Counter(words_edges[word]).most_common():
                if c[0][0] in top_words:
                    a=[word,c[0][0]]
                    a.sort() # to_undirected
                    top_words_edges.append(tuple(a))

    print "%d top words edges"%len(top_words_edges)

    # define acceptable minimum value
    words_minimum_exchange=0
    words_edges_count=[p[1] for p in Counter(top_words_edges).most_common()]

    i=0
    for x in reversed(Counter(words_edges_count).most_common()):
        i+=x[1]
        words_minimum_exchange=x[0]
        if i> words_edges_limit: break
    print "words_minimum_exchange: %d"%words_minimum_exchange

    words_edges_weighted=[
            (p[0][0],p[0][1],p[1]) 
            for p in Counter(top_words_edges).most_common()
            if p[1]>words_minimum_exchange
            ]
    print "Words weighted edges %d"%len(words_edges_weighted)

    wordIndex={}
    indexWords={}
    for i,w in enumerate(top_words): wordIndex[w]=i;

    words_edges_weightedlist=[
                str(wordIndex[w[0]])+" "+str(wordIndex[w[1]])+" "+str(w[2]) 
                for w in words_edges_weighted]    

    Gw = nx.read_weighted_edgelist(words_edges_weightedlist, nodetype=str, delimiter=" ",create_using=nx.DiGraph())

    # dimensions
    Nw,Kw = Gw.order(), Gw.size()
    print "Nodes: ", Nw
    print "Edges: ", Kw

    words_allowed=[top_words[int(w)] for w in Gw.nodes()]
    print "%d words_allowed"%len(words_allowed)

    words_edges_allowed=[(top_words[int(w[0])],top_words[int(w[1])]) for w in Gw.edges()]

    # Average degree
    # words_avg_deg = float(Kw)/Nw
    # print "Average degree: ", words_avg_deg

    # Average clustering coefficient
    # ccsw = nx.clustering(Gw.to_undirected())
    # words_avg_clust_coef = sum(ccsw.values()) / len(ccsw) 
    # print "Average clustering coeficient: %f"%words_avg_clust_coef
        
    # Communities
    words_communities = community.best_partition(Gw.to_undirected()) 
    print "Number of partitions : ", len(set(words_communities.values()))
    # words_modularity=community.modularity(words_communities,Gw.to_undirected())
    # print "Modularity of the best partition: %f"%words_modularity

    # betweeness_centrality
    print "computing betweeness_centrality... (this may take some time)"
    # words_btw_cent=nx.betweenness_centrality (Gw.to_undirected())
    print "computing done"

    # parse data using time reference

    timeframes=[]
    word_median=5
    multi_median=15

    print "processing time frames..."
    for _time in by_time:
    #    if(time=="1346572800"): break # single row for test 
        print 
        print _time, "-"*20
        
        tf=by_time[_time]
        timeframe={}
        
        # get user graph
        timeframe["user_nodes"]=[{
                                  "name":u[0],
                                  "count":u[1], 
                                  "province":user_provinces[u[0]], 
                                  # "btw_cent":users_btw_cent[u[0]],
                                  "community":user_communities[u[0]]
                                  } 
                                 for u in Counter(tf["user_nodes"]).most_common() 
                                 if u[0] in allowed_users]
        
        # print "%d users"%len(timeframe["user_nodes"])
        
        timeframe["user_edges"]=[{
                                  "source":u[0][0],
                                  "target":u[0][1],
                                  "weight":u[1]
                                  } 
                                  for u in Counter(tf["user_edges"]).most_common()
                                  if u[0][0] in allowed_users 
                                  and u[0][1] in allowed_users
                                  and u[1] > users_minimum_exchange]
        
        print "%d users edges"%len(timeframe["user_edges"])
        
        timeframe["provinces_edges"]=[]
        for u in timeframe["user_edges"]:
            try : source=user_provinces[u["source"]]
            except KeyError: pass
            try : target=user_provinces[u["target"]]
            except KeyError: pass    
            if source and target : 
                timeframe["provinces_edges"].append(
                    {"source":source,
                    "target":target, 
                    "weight":u["weight"]}
                    )
        
        print "%d provinces edges"%len(timeframe["provinces_edges"])
        
        timeframe["words_nodes"]=[{
                                    "name":w[0],
                                    "count":w[1],
                                    "community":words_communities[str(wordIndex[w[0]])]
                                    # "btw_cent":words_btw_cent[str(wordIndex[w[0]])],
                                  } 
                                  for w in Counter(tf["words_nodes"]).most_common()
                                  if w[0] in words_allowed]
        
        print "%d words"%len(timeframe["words_nodes"])
        
        words_edges_undirected=[]
        for we in tf["words_edges"]:
            a=[we[0],we[1]]
            a.sort()
            if tuple(a) in words_edges_allowed: words_edges_undirected.append(tuple(a))

        timeframe["words_edges"]=[{ "source":w[0][0],
                                   "target":w[0][1],
                                   "weight":w[1]}
                                    for w in Counter(words_edges_undirected).most_common()
                                    if w[1]>words_minimum_exchange]
        
        print "%d words_edges"%len(timeframe["words_edges"])

        words_2_users=[ 
                (c[0][0],c[0][1],c[1]) for c in Counter(tf["words_to_users"]).most_common() 
                if c[0][0] in words_allowed 
                and c[0][1] in allowed_users
                and c[1]>1 # at least 1 exchange
                ]
        print "%d word to users edges"%len(words_2_users)

        words_2_provinces={}
        i=""
        for w in words_2_users:
            try: 
                p=user_provinces[w[1]]
                i=str(wordIndex[w[0]])+"_"+str(p) # use index instead of word
            except KeyError: 
                i=0
            if(i!=0):
                try : words_2_provinces[i]
                except KeyError: words_2_provinces[i]=0
                words_2_provinces[i]+=w[2]
        
        for wp in words_2_provinces:
            e=wp.split("_")
            w=words_2_provinces[wp]
            
        timeframe["words_provinces"]=[ { 
                                        "word":top_words[int(w.split("_")[0])], # parse word from index
                                        "province":w.split("_")[1],
                                        "weight":words_2_provinces[wp] } 
                                        for w in words_2_provinces
                                    ]
        print "%d provinces interactions"%len(timeframe["words_provinces"])
                    
        timeframes.append({"time":_time, "data":timeframe, "count":tf["count"]})


    print " %d timeframes"%len(timeframes)
    timeframes_file=meme_path+"/"+meme_name+"_timeframes.json"

    with open(timeframes_file, 'w') as outfile:
        json.dump(timeframes, outfile)
        print "json data have been saved to %s"%(timeframes_file)

    meme={
        "name":meme_name,
        "data":timeframes,
        "tweets_count":tweets_count,
        "geoclusters": provinces_communities,
        "provincesCount":users_by_provinces_stats
        }

    db[collection].insert(meme)

    print "data saved to %s collection on mongodb"%collections