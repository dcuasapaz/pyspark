# Ingresar los valores de los parametros

# Rutas de cada proceso
RUTA_LIB=
RUTA_LIB_ORACLE=
RUTA_PYTHON=
# Esquema y tabla en HIVE
HIVEDB=
HIVETABLE=
# Fecha de proceso (yyyyMMdd)
VAL_FECHA_PROCESO=
# Parametro de Oracle
JDBCURL= 
TDDB= 
TDHOST= 
TDPASS= 
TDUSER= 
TDTABLE= 
TDPORT= 
TDSERVICE=

/usr/hdp/current/spark2-client/bin/spark-submit --master local --jars $RUTA_LIB/$RUTA_LIB_ORACLE $RUTA_PYTHON/export_oracle.py --vTable=$HIVEDB.$HIVETABLE --vFechaProceso=$VAL_FECHA_PROCESO --vJdbcUrl=$JDBCURL --vTDDb=$TDDB --vTDHost=$TDHOST --vTDPass=$TDPASS --vTDUser=$TDUSER --vTDTable=$TDTABLE --vTDPort=$TDPORT --vTDService=$TDSERVICE
