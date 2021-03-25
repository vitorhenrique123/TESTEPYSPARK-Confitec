import os
import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col, when, current_timestamp
from pyspark.sql.types import DateType
from s3_tools import get_last_csv_link
from dotenv import load_dotenv

load_dotenv()


defautl_path     = "output"
default_filename = "OriginaisNetflix.parquet"
bucket_name      = os.getenv("AWS_DEFAULT_BUCKET") 
AWS_ACCESS_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY") 
AWS_SECRET_KEY   = os.getenv("AWS_ACCESS_KEY_ID") 

spark = SparkSession \
    .builder \
    .appName("Confitec Spark Test") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
    .config('spark.hadoop.fs.s3a.access.key',AWS_SECRET_KEY ) \
    .config('spark.hadoop.fs.s3a.secret.key', AWS_ACCESS_KEY) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()


netflixFile=spark.read.parquet(default_filename)

netflixFile = (
    netflixFile
    .withColumn("Premiere", unix_timestamp(col("Premiere"), "d-MMM-yy").cast("timestamp").cast(DateType()))
    .withColumn("dt_inclusao", col("dt_inclusao").cast("timestamp"))
)

netflixFile = netflixFile.dropDuplicates()
netflixFile = netflixFile.sort(col("Active").desc(), col("Genre"))

netflixFile = (
    netflixFile
    .withColumn("Seasons", when(col("Seasons") == "TBA", "a ser anunciado").otherwise(col("Seasons")))
    .withColumn("Data de Alteração",current_timestamp())
    .withColumnRenamed("Title", "Título")
    .withColumnRenamed("Genre", "Gênero")
    .withColumnRenamed("GenreLabels", "Gênero Capa")
    .withColumnRenamed("Premiere", "Pré-estreia")
    .withColumnRenamed("Seasons", "Temporadas")
    .withColumnRenamed("SeasonsParsed", "Temporadas Lançadas")
    .withColumnRenamed("EpisodesParsed", "Episódios Lançados")
    .withColumnRenamed("Length", "Duração")
    .withColumnRenamed("MinLength", "Duração Mínima")
    .withColumnRenamed("MaxLength", "Duração Máxima")
    .withColumnRenamed("Active", "Ativo")
    .withColumnRenamed("Table", "Temática")
    .withColumnRenamed("Language", "Idioma")
    .withColumnRenamed("dt_inclusao", "Data de inclusão")
)
netflixFile.select(col("Título"),col("Gênero"), col("Temporadas"), col("Pré-estreia"), col("Idioma"), col("Ativo"), col("Status"), col("Data de inclusão"), col("Data de Alteração")) \
    .repartition(1) \
    .write \
    .format("com.databricks.spark.csv") \
    .option("header", True) \
    .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") \
    .mode("overwrite") \
    .save("s3a://fintectest/%s" % defautl_path )

print ("Checking bucket... ", end="")
print(get_last_csv_link(bucket_name, defautl_path))
