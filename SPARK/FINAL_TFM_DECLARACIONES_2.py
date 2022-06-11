import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

spark = SparkSession\
            .builder \
            .appName("DECLARACIONES_2022") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8,org.elasticsearch:elasticsearch-hadoop:8.2.0") \
            .config('spark.sql.debug.maxToStringFields', 2000) \
            .config('spark.debug.maxToStringFields', 2000) \
            .getOrCreate()
            

#Definición de dataframe de lectura hacia kafka                   
Declaraciones_StreamingDF =spark.readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "192.168.1.43:9092")\
  .option("subscribe", "topic_DECLARACIONES")\
  .load()


#Definición de estrucura para extraer los datos del topic proveniente de KAFKA
esquema = StructType([\
  StructField("ANIO", StringType()),\
  StructField("MES", StringType()),\
  StructField("PROVINCIA", StringType()),\
  StructField("CANTON", StringType()),\
  StructField("CODIGO_SECTOR_N1", StringType()),\
  StructField("VENTAS_NETAS_12", DoubleType()) ,\
  StructField("VENTAS_NETAS_0", DoubleType()) ,\
  StructField("EXPORTACIONES", DoubleType()) ,\
  StructField("COMPRAS_NETAS_12", DoubleType()) ,\
  StructField("COMPRAS_NETAS_0", DoubleType()) ,\
  StructField("IMPORTACIONES", DoubleType()) ,\
  StructField("COMPRAS_RISE", DoubleType()) ,\
  StructField("TOTAL_COMPRAS", DoubleType()) ,\
  StructField("TOTAL_VENTAS", DoubleType())])


    
parsedDF = Declaraciones_StreamingDF\
     .select("value")\
     .withColumn("value", F.col("value").cast(StringType()))\
     .withColumn("parejas", F.from_json(F.col("value"), esquema))\
     .withColumn("ANIO", F.col("parejas.ANIO"))\
     .withColumn("MES", F.col("parejas.MES"))\
     .withColumn("PROVINCIA", F.col("parejas.PROVINCIA"))\
     .withColumn("CANTON", F.col("parejas.CANTON"))\
     .withColumn("CODIGO_SECTOR_N1", F.col("parejas.CODIGO_SECTOR_N1"))\
     .withColumn("VENTAS_NETAS_12", F.col("parejas.VENTAS_NETAS_12"))\
     .withColumn("VENTAS_NETAS_0", F.col("parejas.VENTAS_NETAS_0"))\
     .withColumn("EXPORTACIONES", F.col("parejas.EXPORTACIONES"))\
     .withColumn("COMPRAS_NETAS_12", F.col("parejas.COMPRAS_NETAS_12"))\
     .withColumn("COMPRAS_NETAS_0", F.col("parejas.COMPRAS_NETAS_0"))\
     .withColumn("IMPORTACIONES", F.col("parejas.IMPORTACIONES"))\
     .withColumn("COMPRAS_RISE", F.col("parejas.COMPRAS_RISE"))\
     .withColumn("TOTAL_COMPRAS", F.col("parejas.TOTAL_COMPRAS"))\
     .withColumn("TOTAL_VENTAS", F.col("parejas.TOTAL_VENTAS"))
     
     
def CONEXION_ELASTIC(df,epoch):
    
    es_lectura = {"es.nodes" : "localhost","es.port" : "9200","es.resource" : "declaraciones_2022",
                  "es.read.metadata": "true"
    }
    
    es_modificacion = {"es.mapping.id": "_id","es.mapping.exclude": "_id", 
                      "es.write.operation": "update","es.resource" : "declaraciones_2022"}
   
    #Cálculo de agregados de la data que ingresa por KAFKA
    datos_entrantes=df.select("ANIO","MES","PROVINCIA","CANTON",\
                              "CODIGO_SECTOR_N1",'VENTAS_NETAS_12','VENTAS_NETAS_0',\
                              'EXPORTACIONES','COMPRAS_NETAS_12','COMPRAS_NETAS_0',\
                              'IMPORTACIONES','COMPRAS_RISE','TOTAL_COMPRAS','TOTAL_VENTAS')\
    .groupBy("ANIO","MES","PROVINCIA","CANTON","CODIGO_SECTOR_N1")\
    .agg(F.sum("VENTAS_NETAS_12").alias("VENTAS_NETAS_12"),\
             F.sum("VENTAS_NETAS_0").alias("VENTAS_NETAS_0"),\
             F.sum("EXPORTACIONES").alias("EXPORTACIONES"),\
             F.sum("COMPRAS_NETAS_12").alias("COMPRAS_NETAS_12"),\
             F.sum("COMPRAS_NETAS_0").alias("COMPRAS_NETAS_0"),\
             F.sum("IMPORTACIONES").alias("IMPORTACIONES"),\
             F.sum("COMPRAS_RISE").alias("COMPRAS_RISE"),\
             F.sum("TOTAL_COMPRAS").alias("TOTAL_COMPRAS"),\
             F.sum("TOTAL_VENTAS").alias("TOTAL_VENTAS"))


     #Lectura de datos desde Elastic   
    declaraciones = spark.read.format("org.elasticsearch.spark.sql").options(**es_lectura).load()

    declaraciones=declaraciones.withColumn("ANIO1",F.col("ANIO"))\
     .withColumn("MES1",F.col("MES"))\
     .withColumn("PROVINCIA1",F.col("PROVINCIA"))\
     .withColumn("CANTON1",F.col("CANTON"))\
     .withColumn("CODIGO_SECTOR_N11",F.col("CODIGO_SECTOR_N1"))\
     .withColumn("VENTAS_NETAS_121",F.col("VENTAS_NETAS_12").cast(DoubleType()))\
     .withColumn("VENTAS_NETAS_01",F.col("VENTAS_NETAS_0").cast(DoubleType()))\
     .withColumn("EXPORTACIONES1",F.col("EXPORTACIONES").cast(DoubleType()))\
     .withColumn("COMPRAS_NETAS_121",F.col("COMPRAS_NETAS_12").cast(DoubleType()))\
     .withColumn("COMPRAS_NETAS_01",F.col("COMPRAS_NETAS_0").cast(DoubleType()))\
     .withColumn("IMPORTACIONES1",F.col("IMPORTACIONES").cast(DoubleType()))\
     .withColumn("COMPRAS_RISE1",F.col("COMPRAS_RISE").cast(DoubleType()))\
     .withColumn("TOTAL_COMPRAS1",F.col("TOTAL_COMPRAS").cast(DoubleType()))\
     .withColumn("TOTAL_VENTAS1",F.col("TOTAL_VENTAS").cast(DoubleType()))\
         .select("ANIO1","MES1","PROVINCIA1","CANTON1","CODIGO_SECTOR_N11","VENTAS_NETAS_121","VENTAS_NETAS_01",\
                 "EXPORTACIONES1","COMPRAS_NETAS_121","COMPRAS_NETAS_01","IMPORTACIONES1","COMPRAS_RISE1",\
                     "TOTAL_COMPRAS1","TOTAL_VENTAS1","_metadata._id")


    
    
# #SI SON DATOS YA EXISTENTES, SE PROCEDE A MODIFICAR
    datos_modificables=datos_entrantes.join(declaraciones, (datos_entrantes.ANIO==declaraciones.ANIO1)\
                                      & (datos_entrantes.MES==declaraciones.MES1)\
                                      & (datos_entrantes.PROVINCIA==declaraciones.PROVINCIA1)\
                                      & (datos_entrantes.CANTON==declaraciones.CANTON1)\
                                      & (datos_entrantes.CODIGO_SECTOR_N1==declaraciones.CODIGO_SECTOR_N11)\
                                        , 'inner') 
#SE SUMAN LOS DATOS
    datos_modificables=datos_modificables.withColumn('VENTAS_NETAS_12',F.col('VENTAS_NETAS_12')+F.col('VENTAS_NETAS_121'))\
    .withColumn('VENTAS_NETAS_0',F.col('VENTAS_NETAS_0')+F.col('VENTAS_NETAS_01'))\
    .withColumn('EXPORTACIONES',F.col('EXPORTACIONES')+F.col('EXPORTACIONES1'))\
    .withColumn('COMPRAS_NETAS_12',F.col('COMPRAS_NETAS_12')+F.col('COMPRAS_NETAS_121'))\
    .withColumn('COMPRAS_NETAS_0',F.col('COMPRAS_NETAS_0')+F.col('COMPRAS_NETAS_01'))\
    .withColumn('IMPORTACIONES',F.col('IMPORTACIONES')+F.col('IMPORTACIONES1'))\
    .withColumn('COMPRAS_RISE',F.col('COMPRAS_RISE')+F.col('COMPRAS_RISE1'))\
    .withColumn('TOTAL_COMPRAS',F.col('TOTAL_COMPRAS')+F.col('TOTAL_COMPRAS1'))\
    .withColumn('TOTAL_VENTAS',F.col('TOTAL_VENTAS')+F.col('TOTAL_VENTAS1'))\
    .select('ANIO','MES','PROVINCIA','CANTON','CODIGO_SECTOR_N1'\
            ,'_id','VENTAS_NETAS_12','VENTAS_NETAS_0','EXPORTACIONES','COMPRAS_NETAS_12','COMPRAS_NETAS_0',\
            'IMPORTACIONES','COMPRAS_RISE','TOTAL_COMPRAS','TOTAL_VENTAS')
    
    datos_modificables.write.format("org.elasticsearch.spark.sql") \
            .options(**es_modificacion) \
            .mode('append') \
            .save()

    

  #SI SON DATOS NUEVOS LOS QUE SE CAPTURA SE INSERTAN
    datos_nuevos=datos_entrantes.join(declaraciones, (datos_entrantes.ANIO==declaraciones.ANIO1)\
                                      & (datos_entrantes.MES==declaraciones.MES1)\
                                      & (datos_entrantes.PROVINCIA==declaraciones.PROVINCIA1)\
                                      & (datos_entrantes.CANTON==declaraciones.CANTON1)\
                                      & (datos_entrantes.CODIGO_SECTOR_N1==declaraciones.CODIGO_SECTOR_N11)\
                                        , 'left_anti')\
    .select('ANIO','MES','PROVINCIA','CANTON','CODIGO_SECTOR_N1','VENTAS_NETAS_12','VENTAS_NETAS_0','EXPORTACIONES',\
            'COMPRAS_NETAS_12','COMPRAS_NETAS_0','IMPORTACIONES','COMPRAS_RISE','TOTAL_COMPRAS','TOTAL_VENTAS')
    
   

   
    datos_nuevos.write.format("org.elasticsearch.spark.sql") \
            .mode('append') \
            .option("es.nodes", "http://192.168.1.52:9200") \
            .save("declaraciones_2022")
            


    pass

escritura=parsedDF.writeStream.foreachBatch(CONEXION_ELASTIC).trigger(processingTime='30 seconds').start()

