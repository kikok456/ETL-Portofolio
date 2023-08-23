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
#=========================================# 
#               Parameter                 #
#=========================================#
t0 = time.time()

src_db_ip = os.getenv('ip')
src_db_port = os.getenv('port')
src_user = os.getenv('user')
src_pass = os.getenv('password')
src_db_name = os.getenv('database')
src_tb_name = "history"
db = 'dbnew'
tb = "tbnew"


lastds = get_list_partition(db,tb)
tanggalmulai = datetime.strptime(lastds,'%Y%m%d') + timedelta(days=1)
tanggalmulai = tanggalmulai.strftime('%Y-%m-%d')
tanggalmulai = '{} 00:00:00'.format(tanggalmulai)
tanggalmulai = datetime.strptime(tanggalmulai,'%Y-%m-%d %H:%M:%S') - timedelta(hours=7)

tanggalselesai = datetime.strptime(lastds,'%Y%m%d') + timedelta(days=2)
tanggalselesai = tanggalselesai.strftime('%Y-%m-%d')
tanggalselesai = '{} 00:00:00'.format(tanggalselesai)
tanggalselesai = datetime.strptime(tanggalselesai,'%Y-%m-%d %H:%M:%S') - timedelta(hours=7)

tanggalmulaip = datetime.strptime(lastds,'%Y%m%d') + timedelta(days=1)
tanggalselesaip = datetime.strptime(lastds,'%Y%m%d') + timedelta(days=2)
print("{} sampai {}".format(tanggalmulaip,tanggalselesaip))

max_clock=tanggalmulai.timestamp()
last_date=tanggalselesai.timestamp()

count=0
n= 0
is_truncate = True
oldds = 0

while max_clock < last_date:
  min_clock = datetime.fromtimestamp(max_clock)
  max_clock = min_clock + timedelta(hours=1)
  
  max_clock=max_clock.timestamp()
  min_clock=min_clock.timestamp()
  
  jammulai=datetime.fromtimestamp(min_clock  + (7*3600)).strftime('%c')
  jamselesai=datetime.fromtimestamp(max_clock + (7*3600)).strftime('%c')

  newds = datetime.fromtimestamp(min_clock  + (7*3600)).strftime("%Y%m%d")
  newds
  if newds != oldds :
    print("Ganti hari yaa")
    print("Ke tgl {}".format(newds))
    is_truncate = True
  else :
    is_truncate = False

  oldds = newds
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
  
  print('processing...')
  print('searching between {} - {}'.format(jammulai,jamselesai))
  statement = """
                    (
                      SELECT * FROM {} where clock >= {} and clock < {} ORDER BY clock DESC
                    ) AS final
              """.format(src_tb_name, min_clock,max_clock)
  print(statement)
  df = spark.read.format("jdbc")\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("url","jdbc:mysql://{}:{}/{}".format(src_db_ip, src_db_port, src_db_name))\
    .option("user", src_user )\
    .option("password", src_pass)\
    .option("useSSL", "false")\
    .option("dbtable",statement)\
    .load()
  
  df = df.withColumn("value", col("value").cast(DecimalType(precision=20, scale=0)))
  print("num rows after :",df.count())  
  df = df.withColumn('ds', f.date_format(f.to_date((df.clock + (7*3600)).cast(dataType=t.TimestampType())), 'yyyyMMdd'))

  if df.count() == 0:
    print('no data will write to database')
  else:
    print('append to database ...')
    writeTable(df, db, tb)

  min_clock = max_clock


print('table is up to date, try updating in the next day')

t = datetime(1,1,1)+timedelta(seconds=int(time.time()-t0))
deltatime = "{0}:{1}:{2}".format(str(t.hour).zfill(2),str(t.minute).zfill(2),str(t.second).zfill(2))
print("ETL took {}".format(deltatime))

spark.stop()