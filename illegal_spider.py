import pandas as pd
import gevent
import requests
import lxml.html as lx
import re
# -*- coding: utf-8 -*-

def spider_code():
    codes = []
    fines = []
    scores = []
    contents = []
    urls=["http://www.gzhao123.com/life/gz/jtwfdm10_12/",
          "http://www.gzhao123.com/life/gz/jtwfdm13_17/",
          "http://www.gzhao123.com/life/gz/jtwfdm20_47/",
          "http://www.gzhao123.com/life/gz/jtwfdm50_60/",
          "http://www.gzhao123.com/life/gz/jtwfdm70_76/",
          "http://www.gzhao123.com/life/gz/jtwfdm80_83/",
          ]
    for url in urls:
        response=requests.get(url)
        response.encoding="gbk"
        print(response.text)
        tree=lx.fromstring(response.text)
        code=tree.xpath("//table/tbody//td[1]/text()")
        content=tree.xpath("//table/tbody//td[2]/text()")
        fine=tree.xpath("//table/tbody//td[6]/text()")
        score=tree.xpath("//table/tbody//td[5]/text()")

        code=code[1:]
        fine=fine[1:]
        score=score[1:]
        content=content[1:]
        codes=codes+code
        fines=fines+fine
        scores=scores+score
        contents=contents+content

    illegal_code=pd.DataFrame()
    illegal_code["code"]=codes
    illegal_code["fine"]=fines
    illegal_code["score"]=scores
    illegal_code["content"]=contents
    map={"\n":"","\t":"","\r":""}
    illegal_code=illegal_code.replace(map)
    illegal_code["code"]=illegal_code["code"].str.strip()
    illegal_code["fine"]=illegal_code["fine"].str.strip()
    illegal_code["score"]=illegal_code["score"].str.strip()
    illegal_code["content"]=illegal_code["content"].str.strip()
    illegal_code.to_csv("illegal_code.csv",encoding="utf-8")


spider_code()

