#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import ner
import jieba # import mmseg
import jieba.analyse
from os import path

######################## 
# NLP + NER
# NER server should be up: ../see ner-server/
######################## 

# set relative URLs
here=path.dirname(path.dirname(path.abspath(__file__)))
stopwords_file=here+"/lib/stopwords/zh-stopwords"
dico_file=here+'/lib/dict/dict.txt.big'

# TODO : implement those stopwords
# TODO : add username type "uB5NNODBJ"
weibo_stopwords=["回复","】","【 ","ukn"]

class NLPMiner:
    def __init__(self): 

        print "init NLP toolkit"

        # self.tagger = ner.SocketNER(host='localhost', port=1234)

        # parse list of stopwords
        self.stoplist=[i.strip() for i in open(stopwords_file)]
        self.stoplist+=weibo_stopwords

        # better support for traditional character
        jieba.set_dictionary(dico_file)

    def extract_keywords(self,txt):
        tags = jieba.analyse.extract_tags(txt, 20)
        # dico=extract_dictionary(txt)
        # tags=remove_stopwords(txt)
        return tags

    def extract_dictionary(self,txt):
        seg_list = jieba.cut(txt, cut_all=False)  # 搜索引擎模式
        # print ", ".join(seg_list)
        return list(seg_list)

    def remove_stopwords(self,txt):
        txt_wo_stopwords=[w for w in txt if w.encode('utf-8') not in self.stoplist and w.encode('utf-8') !=" "]
        return txt_wo_stopwords

    # def extract_named_entities_from_dico(self,dico):
    #     # prepare Chinese seg for ENR
    #     # TODO : remove punctuation / stopwords ".","[","]",etc.
    #     seg_str=" ".join(dico)
    #     # get all entities 
    #     tags= self.tagger.get_entities(seg_str)
    #     return tags
