from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
APP_NAME="acccident_data_process"
from pyspark.sql import functions
from pyspark.sql.types import IntegerType


def process_data():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    accident_data = sqlContext.read.csv("accident_data_merged.csv",header=True, encoding="utf-8")

    accident_data = accident_data.drop("Unnamed: 0")
    accident_data = accident_data.drop("_c0")
    accident_data = accident_data.drop("id_x")
    accident_data = accident_data.drop("id_y")
    accident_data = accident_data.drop("id")

    accident_data = accident_data.replace(
        ["白色", "黑色", "红色", "银色", "灰色", "黄色", "绿色", "蓝色", "白色", "黑 ", "北", " 银", "BAI", "棕色"],
        ["白", "黑", "红", "银", "灰", "黄", "绿", "蓝", "白", "黑", "白", "银", "白", "灰"], ["carcolor1", "carcolor2"])
    accident_data = accident_data.replace(["其他", "白", "黑", "银", "红", "蓝", "黄", "绿", "灰"],
                                          ["0", "1", "2", "3", "4", "5", "6", "7", "8"], ["carcolor1", "carcolor2"])
    accident_data = accident_data.replace(["多云", "阵雨", "阴", "小雨", "晴", "中雨", "雷阵雨", "大雨", "暴雨", "冻雨"],
                                          ["0", "1", "2", "3", "4", "5", "6", "7", "8", "8"], ["weather1", "weather2"])

    ageF= functions.udf(get_age,IntegerType())
    accident_data = accident_data.withColumn("driver1_age_category", ageF(accident_data.driver1_age))
    accident_data = accident_data.withColumn("driver2_age_category", ageF(accident_data.driver2_age))

    yearF=functions.udf(get_year,IntegerType())
    accident_data = accident_data.withColumn("driver1_year_category", yearF(accident_data.driver1_years))
    accident_data = accident_data.withColumn("driver2_year_category", yearF(accident_data.driver2_years))

    accident_data = accident_data.replace(["南明区", "云岩区", "乌当区", "花溪区", "观山湖区", "白云区"],
                                          ["0", "1", "2", "3", "4", "5"], "district")
    accident_data = accident_data.replace(["东北风", "南风", "东南风", "东风"], ["1", "2", "3", "4"], ["wind1", "wind2"])
    accident_data = accident_data.replace(["负全部责任", "负同等责任"], ["1", "0"], "driver1responsibility")
    accident_data = accident_data.replace(["不负责任", "负同等责任"], ["1", "0"], "driver2responsibility")

    accident_data = accident_data.fillna("0", "clpp1")
    clpp1 = accident_data.groupby("clpp1").count()
    clpp2 = accident_data.groupby("clpp2").count()
    clpp1 = clpp1.sort("count", ascending=False)
    clpp2 = clpp2.sort("count", ascending=False)
    clpp1 = clpp1.select("clpp1")
    clpp2 = clpp2.select("clpp2")
    clpp1_list = clpp1.take(clpp1.count())
    clpp2_list = clpp2.take(clpp2.count())
    clpp1_list_divided = list(range(4))
    clpp1_list_divided[0] = get_clpp_list(clpp1_list, 0, 1, "clpp1")
    clpp1_list_divided[1] = get_clpp_list(clpp1_list, 1, 10, "clpp1")
    clpp1_list_divided[2] = get_clpp_list(clpp1_list, 10, 40, "clpp1")
    clpp1_list_divided[3] = get_clpp_list(clpp1_list, 40, len(clpp1_list), "clpp1")
    clpp2_list_divided = list(range(4))
    clpp2_list_divided[0] = get_clpp_list(clpp2_list, 0, 1, "clpp2")
    clpp2_list_divided[1] = get_clpp_list(clpp2_list, 1, 10, "clpp2")
    clpp2_list_divided[2] = get_clpp_list(clpp2_list, 10, 40, "clpp2")
    clpp2_list_divided[3] = get_clpp_list(clpp2_list, 40, len(clpp2_list), "clpp2")
    make_clpp1_function=functions.udf(lambda c :get_clpp_num(c,clpp1_list_divided),IntegerType())
    make_clpp2_function=functions.udf(lambda c :get_clpp_num(c,clpp2_list_divided),IntegerType())
    accident_data = accident_data.withColumn("clpp1_category", make_clpp1_function(accident_data.clpp1))
    accident_data = accident_data.withColumn("clpp2_category", make_clpp2_function(accident_data.clpp1))

    jxmc1 = accident_data.groupby("jxmc1").count()
    jxmc2 = accident_data.groupby("jxmc2").count()
    jxmc1 = jxmc1.sort("count", ascending=False)
    jxmc2 = jxmc2.sort("count", ascending=False)
    jxmc1 = jxmc1.select("jxmc1")
    jxmc2 = jxmc2.select("jxmc2")
    jxmc1_list = jxmc1.take(jxmc1.count())
    jxmc2_list = jxmc2.take(jxmc2.count())
    jxmc1_list_divided = list(range(5))
    jxmc2_list_divided = list(range(5))
    jxmc1_list_divided[0] = get_jxmc_list(jxmc1_list, 0, 1, "jxmc1")
    jxmc1_list_divided[1] = get_jxmc_list(jxmc1_list, 1, 2, "jxmc1")
    jxmc1_list_divided[2] = get_jxmc_list(jxmc1_list, 2, 10, "jxmc1")
    jxmc1_list_divided[3] = get_jxmc_list(jxmc1_list, 10, 40, "jxmc1")
    jxmc1_list_divided[4] = get_jxmc_list(jxmc1_list, 40, len(jxmc1_list), "jxmc1")
    jxmc2_list_divided[0] = get_jxmc_list(jxmc2_list, 0, 1, "jxmc2")
    jxmc2_list_divided[1] = get_jxmc_list(jxmc2_list, 1, 2, "jxmc2")
    jxmc2_list_divided[2] = get_jxmc_list(jxmc2_list, 2, 10, "jxmc2")
    jxmc2_list_divided[3] = get_jxmc_list(jxmc2_list, 10, 40, "jxmc2")
    jxmc2_list_divided[4] = get_jxmc_list(jxmc2_list, 40, len(jxmc2_list), "jxmc2")
    make_jxmc1_function=functions.udf(lambda c :get_jxmc_num(c,jxmc1_list_divided),IntegerType())
    make_jxmc2_function=functions.udf(lambda c :get_jxmc_num(c,jxmc2_list_divided),IntegerType())
    accident_data = accident_data.withColumn("jxmc1_category", make_jxmc1_function(accident_data.jxmc1))
    accident_data = accident_data.withColumn("jxmc2_category", make_jxmc2_function(accident_data.jxmc1))

    accident_data = accident_data.withColumn("temperature",(accident_data.temperature_min + accident_data.temperature_max) / 2)
    cols = ["driver1fault", "sex1", "carcolor1", "clpp1", "jxmc1",
            "sex2", "carcolor2", "clpp2", "jxmc2",
            "accident_month", "accident_quarter", "accident_weekday", "accident_day", "accident_hour",
            "accident_minute",
            "is_province1", "is_city1", "is_driver1_city", "is_driver1_province",
            "is_province2", "is_city2", "is_driver2_city", "is_driver2_province",
            "weather1", "weather2", "wind1", "wind2", "district", "lng", "lat",
            "fine_x", "score_x", "maxtime_x", "xfcount_x", "wfxw_x"]
    accident_data = turn_type(accident_data, cols)

    return accident_data


def turn_type(df, cols):
    for col in cols:
        df = df.withColumn(col + "Tmp", df[col].cast(IntegerType())).drop(col).withColumnRenamed(col + "Tmp", col)
    return df

def get_age(age):
    if age:
        age=float(age)
        if (age<30):
            return 0
        elif (age>29 and age<45) :
            return 1
        elif (age>44 and age<60):
            return 2
        else :
            return 3
    else:
        return 3

def get_year(year):
    if year:
        year=float(year)
        if (year<4):
            return 0
        elif (year>3 and year<11) :
            return 1
        elif (year>10 and year<21):
            return 2
        else :
            return 3
    else:
        return 0

def get_clpp_list(clpp_list,start,end,c):
    clpp=[]
    for i in range(start,end):
        clpp.append(clpp_list[i].asDict()[c])
    return clpp

def get_clpp_num(clpp,clpp_list_divided):
    for i in range(4):
        if clpp in clpp_list_divided[i]:
            return i
        else :continue

def get_jxmc_list(jxmc_list,start,end,c):
    jxmc=[]
    for i in range(start,end):
        jxmc.append(jxmc_list[i].asDict()[c])
    return jxmc

def get_jxmc_num(jxmc,jxmc_list_divided):
    for i in range(5):
        if jxmc in jxmc_list_divided[i]:
            return i
        else :continue