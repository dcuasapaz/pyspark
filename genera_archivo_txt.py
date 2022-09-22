import sys
reload(sys)
sys.setdefaultencoding('UTF-8')
from pyspark.sql import SparkSession
import pandas as pd
import datetime
from pyspark.sql import functions as F, Window
import re
import argparse
from pyspark.sql.functions import *

parser = argparse.ArgumentParser()
parser.add_argument('--vRuta', required=True, type=str)
parser.add_argument('--vTabla', required=True, type=str)
parametros = parser.parse_args()
vRuta=parametros.vRuta
vTabla=parametros.vTabla

timestartmain = datetime.datetime.now() 

spark = SparkSession\
	.builder\
	.appName("Generar txt")\
	.master("local")\
	.enableHiveSupport()\
	.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


vSql = """

SELECT 
-- COLUMNAS TABLA
FROM {vTabla}

"""

timestart = datetime.datetime.now()
print ("==== Generando archivo "+vRuta+" ====")
df_final=spark.sql(vSql.format(vTabla=vTabla))
pandas_df = df_final.toPandas()
pandas_df.rename(columns = lambda x:x.upper(), inplace=True )
pandas_df.to_csv(vRuta, sep='|',index=False)
timeend = datetime.datetime.now()
duracion = timeend - timestart
print("Generacion exitosa del archivo "+vRuta)
print("Duracion genera archivo "+vRuta+" {}".format(duracion))

spark.stop()
timeendmain = datetime.datetime.now()
duracion = timestartmain- timeendmain 
print("Duracion: {vDuracion}".format(vDuracion=duracion))
