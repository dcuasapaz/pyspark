#########################################################################################################
# NOMBRE: export_oracle.py	      												                        
# DESCRIPCIÓN:																							                                            
# Hive to Oracle			                                                   											              
# AUTOR: Diego Cuasapaz             														                          
# FECHA CREACIÓN: 2022-09-22																			                                      							        		                    					                	
#########################################################################################################

import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse

timestart = datetime.datetime.now() 

parser = argparse.ArgumentParser()
parser.add_argument('--vTable', required=True, type=str)
parser.add_argument('--vFechaProceso', required=True, type=int)
parser.add_argument('--vJdbcUrl', required=True, type=str)
parser.add_argument('--vTDDb', required=True, type=str)
parser.add_argument('--vTDHost', required=True, type=str)
parser.add_argument('--vTDPass', required=True, type=str)
parser.add_argument('--vTDUser', required=True, type=str)
parser.add_argument('--vTDTable', required=True, type=str)
parser.add_argument('--vTDPort', required=True, type=str)
parser.add_argument('--vTDService', required=True, type=str)
parametros = parser.parse_args()
vTable=parametros.vTable
vFechaProceso=parametros.vFechaProceso
vJdbcUrl=parametros.vJdbcUrl       
vTDDb=parametros.vTDDb          
vTDHost=parametros.vTDHost        
vTDPass=parametros.vTDPass        
vTDUser=parametros.vTDUser        
vTDTable=parametros.vTDTable       
vTDPort=parametros.vTDPort        
vTDService=parametros.vTDService

spark = SparkSession\
    .builder\
    .appName("EXPORT ORACLE")\
    .enableHiveSupport()\
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


vSQL="""

SELECT 
-- Columns HIVE
FROM {vTable}
WHERE pt_fecha_proceso={vFechaProceso}

"""

df0 = spark.sql(vSQL.format(vTable=vTable,vFechaProceso=vFechaProceso))
df0.write.format('jdbc').options(
      url=vJdbcUrl,
      driver='oracle.jdbc.driver.OracleDriver',
      dbtable=vTDTable,
      user=vTDUser,
      password=vTDPass).mode('overwrite').save()

spark.stop()

timeend = datetime.datetime.now()
duracion = timeend - timestart
print("INFO: Duracion proceso {}".format(duracion))
