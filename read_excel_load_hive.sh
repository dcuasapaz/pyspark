# Ingresar los valores de los parametros

# Ruta principal de ejecucion
VAL_RUTA=
# Nombre del archivo .xlsx
VAL_ARCHIVO=
# Esquema HIVE
VAL_ESQUEMA_MAIN=
# Tabla HIVE
VAL_TABLA=

/usr/hdp/current/spark2-client/bin/spark-submit --master local $VAL_RUTA/python/read_excel_load_hive.py --rutain=$VAL_ARCHIVO --tablaout=$VAL_ESQUEMA_MAIN.$VAL_TABLA --tipo=overwrite

