#=========================================# 
#           Import Dependencies           #
#=========================================#
import time, os, pytz
import pandas as pd
from datetime import datetime, date
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
#=========================================#
#              Pandas Config              #
#=========================================#
pd.options.display.html.table_schema=True
pd.options.display.max_columns=999
pd.options.display.max_rows=999
#=========================================# 
#          Function get latest DS         #
#=========================================#

def get_list_partition(schema,table):
  partitions = spark.sql("""
    SHOW PARTITIONS {}.{}
  """.format(schema,table)).sort('partition', ascending=False).collect()
  if len(partitions) != 0:
    list_partition = []
    row = partitions [0]
    print(row[0].split('=')[1])
    return row[0].split('=')[1]

def writeTable(df, db, table, mode='APPEND'):
    df\
      .sample(False,0.1,None)\
      .write\
      .format("parquet")\
      .mode("overwrite")\
      .saveAsTable("sample1_{}".format(table))
    path = lambda p: spark._jvm.org.apache.hadoop.fs.Path(p)
    try:
      fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
      hdfs_folder = fs.getContentSummary(path("{}sample1_{}/".format(pathf,table)))
      repartition_number = float(hdfs_folder.getLength()*10)/float(128*1024*1024)
      repartition_number = int(1 if repartition_number < 1 else repartition_number)
    except:
      print('Table Tidak Ada')
      repartition_number = 1

    spark.sql("DROP TABLE sample1_{}".format(table))
    df = df.repartition(repartition_number)
    if mode.upper() == "OVERWRITE":
      df\
        .write\
        .format("parquet")\
        .mode("overwrite")\
        .saveAsTable("{}.{}".format(db,table))
    elif mode.upper() == "APPEND":
      df\
        .write\
        .partitionBy("ds")\
        .format("parquet")\
        .mode("append")\
        .saveAsTable("{}.{}".format(db,table))
    spark.sql("REFRESH TABLE {}.{}".format(db, table)) # refresh tabel di spark


#=========================================# 
#          Membuat Session Spark          #
#=========================================#
spark = SparkSession \
        .builder \
        .appName("Update zabbix_tb_history_uint") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "16g") \
        .config("spark.network.timeout", 60) \
        .config("spark.yarn.executor.memoryOverhead", "4g")\
        .enableHiveSupport() \
        .getOrCreate()
print("Spark version : " + spark.version)

##########################################
#                 Source                 #
##########################################
t0 = time.time()    
tb = "tb_processing"
tb_src = "tbnew"
db = "dbnew"

##########################################
#           Waktu (Auto)                 #
##########################################

tanggalmulai = get_list_partition(db,tb)
tanggalmulai = datetime.strptime(tanggalmulai,'%Y%m%d') + timedelta(days=1)
tanggalmulai = tanggalmulai.strftime("%Y-%m-%d")

tanggalselesai = get_list_partition(db,tb_src)
tanggalselesai = datetime.strptime(tanggalselesai,'%Y%m%d')
tanggalselesai = tanggalselesai.strftime("%Y-%m-%d")

tanggalmulai = datetime.strptime(tanggalmulai,'%Y-%m-%d')
tanggalselesai = datetime.strptime(tanggalselesai,'%Y-%m-%d')

print("Ingest Dari {} sampai {}".format(tanggalmulai,tanggalselesai))

##########################################
#                 Looping                #
##########################################

is_truncate = True
oldds = 0


while tanggalmulai <= tanggalselesai:

  tanggalmulaiplus = tanggalmulai + timedelta(days=1)

  jammulai = tanggalmulai.strftime("%Y%m%d")
  jamselesai = tanggalmulaiplus.strftime("%Y%m%d")  
  timeingest = tanggalmulai.time()
  jamselesai

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
  try:
    if is_truncate :
      delete_1_day_before1 = "TRUNCATE TABLE {}.{} partition (ds = '{}')".format(db,tb, newds)
      print(delete_1_day_before1)
      delete_1_day_before1 = spark.sql(delete_1_day_before1)
      print("selesai")
    is_truncate = False
    print('selesai set to false')

  except Exception as e:
    print(e)
  
  print('Read Data')
  pattern_description = r'^[^(]*\\(([^)]*)\\).*$'
  pattern_direction = r'^net\\.if\\.([^\\[]+)\\[.*$'
  qrgetdata = spark.sql(f"""
  SELECT c.item as item
  , regexp_extract(c.name, '{pattern_description}', 1) description
  , regexp_extract(c.key_, '{pattern_direction}', 1) direction,
  substring_index(c.name, ':', 1) as interface,
  b.host as host,
  concat(a.tipe ,' - ',a.ip ,' - ', a.remote) as tipe_ip_remote ,
  a.tipe as tipe,
  a.kanca as kanca,
  a.kanwil as kanwil,
  a.remote as remote,
  a.latitude as latitude,
  a.longitude as longitude,
  a.alamat as alamat,
  a.id_remote as id_remote,
  a.ip as ip,
  d.value as val_max,
  DATE_FORMAT(FROM_UNIXTIME(d.clock), 'YYYY-MM-dd HH:mm:00') as waktu,
  d.ds as ds
  FROM dbnew.tb_r a
  INNER JOIN dbnew.tb_h b on a.ip=b.host 
             and a.tipe in ('tipe')
  INNER JOIN dbnew.tb_i  c on b.host = c.host
             and (c.key_ like 'net.if.in[%]' or c.key_ like 'net.if.out[%]')
             and c.key_ not like 'net.if.%[%{{{{{{{{%}}}}}}}}]'
             and regexp_extract(c.name, '{pattern_description}', 1) <> ''
             and regexp_extract(c.name, '{pattern_description}', 1) not like 'Tunne%'
  INNER JOIN tbnew.dbnew d on c.item=d.item
  where d.ds='{jammulai}'
    """)
#  qrgetdata.show(10)

  try:
    print('Processing Data')
    qrgetdata.registerTempTable("qrgetdata")
    
    df_zabbixharianfinal=spark.sql("""select a.interface,
                                             a.kanwil,
                                             a.kanca,
                                             a.remote,
                                             a.item,
                                             a.id_remote,
                                             a.ip,
                                             a.tipe,
                                             a.alamat,
                                             a.tipe_ip_remote,
                                             a.latitude,
                                             a.longitude,
                                             a.val_max as throughput_in,
                                             b.val_max as throughput_out,
                                             waktu
                                     from (select * from qrgetdata where direction = 'in') as a
                                     full join (select * from qrgetdata where direction = 'out') as b
                                     using (host, description, waktu)""")

    df_zabbixharianfinal = df_zabbixharianfinal.withColumn("waktu", df_zabbixharianfinal.waktu + f.expr('INTERVAL 7 HOURS'))

    df_zabbixharianfinal = df_zabbixharianfinal.withColumn('throughput_bps',col('throughput_in')+col('throughput_out'))\
                                               .withColumn("tanggal_bulan_tahun",F.substring(F.col("waktu"), 1, 10))\
                                               .withColumn("jam",F.substring(F.col("waktu"), 12, 2))\
                                               .withColumn("menit",F.substring(F.col("waktu"), 15, 2))\
                                               .withColumn('ds', lit(newds))
   
    
    df_zabbixharianfinal=df_zabbixharianfinal.select('interface',\
                                                     'kanwil',\
                                                     'kanca',\
                                                     'remote',\
                                                     'item',\
                                                     'id_remote',\
                                                     'ip',\
                                                     'tipe',\
                                                     'alamat',\
                                                     'tipe_ip_remote',\
                                                     'latitude',\
                                                     'longitude',\
                                                     'waktu',\
                                                     'tanggal_bulan_tahun',\
                                                     'jam',\
                                                     'menit',\
                                                     'throughput_in',\
                                                     'throughput_out',\
                                                     'throughput_bps',\
                                                     'ds')

    table=True
    
  except Exception as e:
    print(e)
    table=False

  
  if table==True:
    print("Mulai masukin ke hive")
    writeTable(df_zabbixharianfinal, db, tb_processing)
    print("Selesai masukin ke hive")
  else:
    print("Data Kosong")

  tanggalmulai = tanggalmulaiplus
      
t = datetime(1,1,1)+timedelta(seconds=int(time.time()-t0))
deltatime = "{0}:{1}:{2}".format(str(t.hour).zfill(2),str(t.minute).zfill(2),str(t.second).zfill(2))
print("ETL took {}".format(deltatime))



spark.stop()

