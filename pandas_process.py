import numpy as np
import pandas as pd

def process_data():
    accident_data = pd.read_csv("accident_data_merged.csv", encoding="utf-8")
    fault10_index = accident_data[accident_data["driver1fault"] == 10].index
    accident_data = accident_data.drop(fault10_index, axis=0)
    accident_data = accident_data.drop("Unnamed: 0", axis=1)
    accident_data = accident_data.drop("Unnamed: 0.1", axis=1)
    accident_data = accident_data.drop(["id", "id_x", "id_y"], axis=1)

    weather_map = {"多云": 0, "阵雨": 1, "阴": 2, "小雨": 3, "晴": 4, "中雨": 5, "雷阵雨": 6,
                   "大雨": 7, "暴雨": 8, "冻雨": 8}
    accident_data["weather2"] = accident_data["weather2"].replace(weather_map)
    accident_data["weather1"] = accident_data["weather1"].replace(weather_map)

    color_map = {"白色": "白", "黑色": "黑", "红色": "红", "银色": "银", "灰色": "灰",
                 "黄色": "黄", "绿色": "绿", "蓝色": "蓝", " 白色": "白", "黑 ": "黑",
                 "北": "白", " 银": "银", "BAI": "白", "棕色": '灰'}
    carcolor1 = accident_data["carcolor1"]
    carcolor2 = accident_data["carcolor2"]

    carcolor1 = carcolor1.replace(color_map)
    carcolor2 = carcolor2.replace(color_map)
    other1 = carcolor1.value_counts()[8:].index
    carcolor1 = carcolor1.replace(other1, "其他")
    other2 = carcolor2.value_counts()[8:].index
    carcolor2 = carcolor2.replace(other2, "其他")
    accident_data["carcolor1"] = carcolor1
    accident_data["carcolor2"] = carcolor2
    colormap2 = {"白": 1, "黑": 2, "银": 3, "红": 4, "蓝": 5, "黄": 6, "绿": 7, "灰": 8, "其他": 0}
    accident_data["carcolor1"] = accident_data["carcolor1"].replace(colormap2)
    accident_data["carcolor2"] = accident_data["carcolor2"].replace(colormap2)

    accident_data["driver1_age_category"] = accident_data["driver1_age"].apply(get_age)
    accident_data["driver2_age_category"] = accident_data["driver2_age"].apply(get_age)

    driver1_years_mean = accident_data["driver1_years"].mean()
    driver1_years_std = accident_data["driver1_years"].std()
    age_null_count = accident_data["driver1_years"].isnull().sum()
    age_null_random_list = np.random.randint \
        (driver1_years_mean - driver1_years_std, driver1_years_mean + driver1_years_std, age_null_count)
    accident_data["driver1_years"][np.isnan(accident_data["driver1_years"])] = age_null_random_list
    accident_data["driver1_years"] = accident_data["driver1_years"].astype(int)
    accident_data["driver1_year_category"] = accident_data["driver1_years"].apply(get_year)

    accident_data["driver2_years"]=accident_data["driver2_years"].replace(-27,np.nan)
    driver2_years_mean = accident_data["driver2_years"].mean()
    driver2_years_std = accident_data["driver2_years"].std()

    age_null_count = accident_data["driver2_years"].isnull().sum()
    age_null_random_list = np.random.randint \
        (driver2_years_mean - driver2_years_std, driver2_years_mean + driver2_years_std, age_null_count)
    accident_data["driver2_years"][np.isnan(accident_data["driver2_years"])] = age_null_random_list
    accident_data["driver2_years"] = accident_data["driver2_years"].astype(int)
    accident_data["driver2_year_category"] = accident_data["driver2_years"].apply(get_year)

    district_map = {"南明区": 0, "云岩区": 1, "乌当区": 2, "花溪区": 3, "观山湖区": 4, "白云区": 5}
    accident_data["district"] = accident_data["district"].replace(district_map)

    clpp1_count = accident_data["clpp1"].value_counts()
    accident_data["clpp1"] = accident_data["clpp1"].replace("-1", 0)
    accident_data["clpp1"] = accident_data["clpp1"].replace(clpp1_count.index[1:10], 1)
    accident_data["clpp1"] = accident_data["clpp1"].replace(clpp1_count.index[10:40], 2)
    accident_data["clpp1"] = accident_data["clpp1"].replace(clpp1_count.index[40:], 3)

    clpp2_count = accident_data["clpp2"].value_counts()
    accident_data["clpp2"] = accident_data["clpp2"].replace("-1", 0)
    accident_data["clpp2"] = accident_data["clpp2"].replace(clpp2_count.index[1:10], 1)
    accident_data["clpp2"] = accident_data["clpp2"].replace(clpp2_count.index[10:40], 2)
    accident_data["clpp2"] = accident_data["clpp2"].replace(clpp2_count.index[40:], 3)

    jxmc1_count = accident_data["jxmc1"].value_counts()
    accident_data["jxmc1"] = accident_data["jxmc1"].replace(jxmc1_count.index[0], 0)
    accident_data["jxmc1"] = accident_data["jxmc1"].replace(jxmc1_count.index[1], 1)
    accident_data["jxmc1"] = accident_data["jxmc1"].replace(jxmc1_count.index[2:10], 2)
    accident_data["jxmc1"] = accident_data["jxmc1"].replace(jxmc1_count.index[10:40], 3)
    accident_data["jxmc1"] = accident_data["jxmc1"].replace(jxmc1_count.index[40:], 4)

    jxmc2_count = accident_data["jxmc2"].value_counts()
    accident_data["jxmc2"] = accident_data["jxmc2"].replace(jxmc2_count.index[0], 0)
    accident_data["jxmc2"] = accident_data["jxmc2"].replace(jxmc2_count.index[1], 1)
    accident_data["jxmc2"] = accident_data["jxmc2"].replace(jxmc2_count.index[2:10], 2)
    accident_data["jxmc2"] = accident_data["jxmc2"].replace(jxmc2_count.index[10:40], 3)
    accident_data["jxmc2"] = accident_data["jxmc2"].replace(jxmc2_count.index[40:], 4)

    accident_data["driver1responsibility"] = accident_data["driver1responsibility"].replace("负全部责任", 1)
    accident_data["driver1responsibility"] = accident_data["driver1responsibility"].replace("负同等责任", 0)
    accident_data["driver2responsibility"] = accident_data["driver2responsibility"].replace("不负责任", 1)
    accident_data["driver2responsibility"] = accident_data["driver2responsibility"].replace("负同等责任", 0)

    wind_map={"东北风":1,"南风":2,"东南风":3,"东风":4}
    accident_data["wind1"]=accident_data["wind1"].replace(wind_map)
    accident_data["wind2"]=accident_data["wind2"].replace(wind_map)

    accident_data["is_province1"] = accident_data["is_province1"].astype(int)
    accident_data["is_city1"] = accident_data["is_city1"].astype(int)
    accident_data["is_driver1_city"] = accident_data["is_driver1_city"].astype(int)
    accident_data["is_driver1_province"] = accident_data["is_driver1_province"].astype(int)

    accident_data["is_province2"] = accident_data["is_province2"].astype(int)
    accident_data["is_city2"] = accident_data["is_city2"].astype(int)
    accident_data["is_driver2_city"] = accident_data["is_driver2_city"].astype(int)
    accident_data["is_driver2_province"] = accident_data["is_driver2_province"].astype(int)

    accident_data["accidenttime"] = pd.to_datetime(accident_data["accidenttime"])
    accident_data["maxtime_x"] = pd.to_datetime(accident_data["maxtime_x"])
    accident_data["difftime_x"] = accident_data["accidenttime"] - accident_data["maxtime_x"]
    accident_data["difftime_x"] = accident_data["difftime_x"].dt.days

    accident_data["maxtime_y"] = pd.to_datetime(accident_data["maxtime_y"])
    accident_data["difftime_y"] = accident_data["accidenttime"] - accident_data["maxtime_y"]
    accident_data["difftime_y"] = accident_data["difftime_y"].dt.days

    accident_data["driver1license"] = accident_data["driver1license"]. \
        replace("***", "000000000")
    accident_data["driver2license"] = accident_data["driver2license"]. \
        replace("***", "000000000")
    accident_data = accident_data.drop(
        accident_data[accident_data["driver1license"].str.contains("000000000") == True].index, axis=0)
    accident_data = accident_data.drop(
        accident_data[accident_data["driver2license"].str.contains("000000000") == True].index, axis=0)

    license2_list = accident_data["driver2license"].value_counts()[:32].index
    for l in license2_list:
        accident_data = accident_data.drop(accident_data[accident_data["driver2license"].str.contains(l) == True].index,
                                           axis=0)

    accident_data["fine_x"] = accident_data["fine_x"].fillna(0)
    accident_data["score_x"] = accident_data["score_x"].fillna(0)
    accident_data["wfxw_x"] = accident_data["wfxw_x"].fillna(0)
    accident_data["xfcount_x"] = accident_data["xfcount_x"].fillna(0)

    accident_data["fine_y"] = accident_data["fine_y"].fillna(0)
    accident_data["score_y"] = accident_data["score_y"].fillna(0)
    accident_data["wfxw_y"] = accident_data["wfxw_y"].fillna(0)
    accident_data["xfcount_y"] = accident_data["xfcount_y"].fillna(0)

    return accident_data

def get_age(age):
    if (age < 30):
        return 0
    elif (age > 29 and age < 45):
        return 1
    elif (age > 44 and age < 60):
        return 2
    else:
        return 3

def get_year(year):
    if year<4:
        return 0
    elif (year>3 and year<11):
        return 1
    elif (year>10 and year<21):
        return 2
    else:
        return 3