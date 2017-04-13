import pandas as pd
import gevent
import requests
import json
import time
class Map_info():
    def __init__(self):
        accident_data=pd.read_csv("traffic_accident_data.csv")
        self.accidentaddr=accident_data["accidentaddr"][50000:]
        self.map_lngs=[]
        self.map_lats=[]
        self.map_infos=[]


    def spider_mapinfo(self,index,map_data):
        print("第"+str(index)+"个数据")
        print(map_data)
        try:
            map_re = requests.get("http://api.map.baidu.com/place/search?&query=" +
                          map_data + "&region=贵阳&output=json&pagesize=5&coord_type=1"
                                     "&key=23bdc633acdebc386573c30a7af665bd")
            print(map_re.status_code)
            self.map_infos.append(map_re.text)
        except :
            text="error"
            self.map_infos.append(text)


    def spider_start(self):
        map_spider=[]
        for index,map_data in enumerate(self.accidentaddr):
            map_spider.append(gevent.spawn(self.spider_mapinfo,index,map_data))
        gevent.joinall(map_spider)
        self.save_data()

    def save_data(self):
        map_data=pd.DataFrame()
        map_data["map_infos"]=self.map_infos
        map_data.to_csv("map_data6.csv",encoding='utf-8',index_label="ID")

    def process_map(self):
        map_data=pd.read_csv("map_data6.csv")
        for index,map_info in enumerate(map_data["map_infos"]):
            print(index)
            # map_info.replace('""','"')
            map_info = map_info.replace("\\", '/')
            map_info = map_info.replace("\2", '/2')
            map_info = map_info.replace('""', '"')
            try:
                map_info = json.loads(map_info)
            except :
                print("错误")
                self.map_lats.append(26.33)
                self.map_lngs.append(106.62)

            if (len(map_info['results']) != 0):
                map_lat = map_info['results'][0]["location"]['lat']
                map_lng = map_info['results'][0]["location"]['lng']
                print(map_lat)
                print(map_lng)
                print('\n')
                self.map_lats.append(map_lat)
                self.map_lngs.append(map_lng)
            else:
                print("第" + str(index) + "个错误")
                self.map_lats.append(26.33)
                self.map_lngs.append(106.62)

        map_data["map_lats"]=self.map_lats
        map_data["map_lngs"]=self.map_lngs
        map_data.to_csv("map_data.csv", encoding='utf-8')



map_test=Map_info()
stime=time.time()
print(time.time())
map_test.spider_start()
map_test.process_map()
etime=time.time()
print(etime)
print(etime-stime)
