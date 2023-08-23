import time, os, pytz
import pandas as pd
import requests
import io
from datetime import datetime, date
from pyspark.sql import functions as f
from pyspark.sql import SparkSession, functions as F, types as T, window as W
from pyspark.sql import types as t
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import col,to_timestamp,date_format, from_unixtime,year,month,to_date
from pyspark.sql.types import DecimalType
import numpy as np
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

spark = SparkSession \
        .builder \
        .appName("Update zabbix_tb_history_uint") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "16g") \
        .config("spark.network.timeout", 60) \
        .config("spark.yarn.executor.memoryOverhead", "7g")\
        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")\
        .enableHiveSupport() \
        .getOrCreate()
print("Spark version : " + spark.version)

#=========================================# 
#                Link CSV                 #
#=========================================#

link_appId = "[Directory]"

#=========================================# 
#               Get App Id                #
#=========================================#
df_app_id = pd.read_csv(link_appId)
    
#=========================================# 
#               Parameter                 #
#=========================================#
t0 = time.time()
table = "raw_table"
schema = "test"

##########################################
#             Waktu (Auto)               #
##########################################

tanggalmulai=datetime.now() + timedelta(hours=6)
tanggalselesai=datetime.now() + timedelta(hours=7)

tanggalmulai=tanggalmulai.strftime('%Y-%m-%d %H:00:00')
tanggalselesai=tanggalselesai.strftime('%Y-%m-%d %H:00:00')
tanggalmulai = datetime.strptime(tanggalmulai,'%Y-%m-%d %H:%M:%S')
tanggalselesai = datetime.strptime(tanggalselesai,'%Y-%m-%d %H:%M:%S')

resolusi = "FIVE_MINUTE"
###############################################################

is_truncate = True
oldds = 0

print("{} sampai {}".format(tanggalmulai,tanggalselesai))

while tanggalmulai < tanggalselesai:

  tanggalmulaiplus = tanggalmulai + timedelta(minutes=5)

  jammulai = tanggalmulai.strftime("%Y-%m-%d_%H:%M:%S")
  jamselesai = tanggalmulaiplus.strftime("%Y-%m-%d_%H:%M:%S")  
  timeingest = tanggalmulai.time()  

  newds = tanggalmulai.strftime("%Y%m%d")
  if newds != oldds :
    print("Ganti hari yaa")
    print("Ke tgl {}".format(newds))
    is_truncate = True
  else :
    is_truncate = False

  oldds = newds

  print("Proeccesing Ingestion from {} to {} ".format(jammulai, jamselesai))
  print("Dont interrupt before the processing end")
  
  xml = """
  <GenericClientQuery>
    <NetworkObjectData>
        <NetworkParameter>APPLICATION</NetworkParameter>
        <SelectColumnList>
            <ClientColumn>aplikasi</ClientColumn>
            <ClientColumn>titik</ClientColumn>
            <ClientColumn>transactions</ClientColumn>
            <ClientColumn>delay</ClientColumn>
            <ClientColumn>throughput</ClientColumn>
            <ClientColumn>waktu</ClientColumn>
        </SelectColumnList>
    </NetworkObjectData>
    <FlowFilterList>
        <FlowFilter>
            <IPAddress>[IP ADDRESS]</IPAddress>
            <Ifn>[interface]</Ifn>
            <FilterList></FilterList>
        </FlowFilter>
    </FlowFilterList>
    <FunctionList></FunctionList>
    <TimeDef>
        <startTime>{}</startTime>
        <endTime>{}</endTime>
        <resolution>{}</resolution>
    </TimeDef>
    <params>
        <stringConversion>true</stringConversion>
    </params>
  </GenericClientQuery>""".format(jammulai, jamselesai, resolusi)
  headers = {'Content-Type': 'application/xml',
             'Accept': 'text/csv'}
  try:
    kssi_table1 = requests.post('[URL]', data=xml, headers=headers,verify=False,auth=('[username]','[password]')).text

    data_kssi_table1 = io.StringIO(kssi_table1)
    pd_kssi_table1 = pd.read_csv(data_kssi_table1, sep=",")
    pd_kssi_table1 = pd_kssi_table1[pd_kssi_table1['appId_String'].isin(list(df_app_id['app_string']))]  
    pd_kssi_table1['ds'] = newds

    #### Convert to Spark Dataframe ####
    df1 = spark.createDataFrame(pd_kssi_table1)
    df1 = df1.selectExpr("aplikasi",\
                         "titik",\
                         "transaksi",\
                         "delay",\
                         "trhoughput",\
                         "waktu",\
                         "ds")
    
    df1 = df1.withColumn("waktu", to_timestamp("waktu", "E MMM d HH:mm:ss z yyyy")+ f.expr('INTERVAL 7 HOURS'))
    df1 = df1.withColumn("waktu", date_format(from_unixtime(df1.target_time_string.cast("long")), "dd-MM-yyyy HH:mm:ss"))
    df1 = df1.withColumn("waktu_string",F.substring(F.col("waktu"), 1, 10))\
             .withColumn("tahun",F.substring(F.col("waktu"), 7, 4))\
             .withColumn("bulan",F.substring(F.col("waktu"), 4, 2))\
             .withColumn("tanggal",F.substring(F.col("waktu"), 1, 2))\
             .withColumn("jam",F.substring(F.col("waktu"), 12, 2))\
             .withColumn("menit",F.substring(F.col("waktu"), 15, 2))

    table=True
  except Exception as e:
    print(e)
    table=False
  
  if table==True:
    print("Mulai masukin ke hive DF1")
    writeTable(df1, schema, table)
    print("Selesai masukin ke hive")
  else:
    print("Data Kosong")

  tanggalmulai = tanggalmulaiplus
  
    
t = datetime(1,1,1)+timedelta(seconds=int(time.time()-t0))
deltatime = "{0}:{1}:{2}".format(str(t.hour).zfill(2),str(t.minute).zfill(2),str(t.second).zfill(2))
print("ETL took {}".format(deltatime))

spark.stop()
