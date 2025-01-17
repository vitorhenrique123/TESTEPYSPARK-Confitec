import os
import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, IllegalArgumentException, ParseException, StreamingQueryException, QueryExecutionException, PythonException, UnknownException
from pyspark.sql.functions import unix_timestamp, col, when, current_timestamp
from pyspark.sql.types import DateType
from s3_tools import get_last_csv_link
from dotenv import load_dotenv

load_dotenv()


defautl_path     = "output"
default_filename = "OriginaisNetflix.parquet"
envconfig        = {
    "AWS_DEFAULT_BUCKET": '',
    "AWS_SECRET_ACCESS_KEY": '',
    "AWS_ACCESS_KEY_ID": ''
}

"""
 Generates and validates all necessary environment variables
"""

for x in envconfig:
    if  not os.getenv(x):
        print("Misiing %s env variable" % x)
        exit(1)
    else:
        envconfig[x] = os.getenv(x) 
        
"""
 Valid if the data source file exists
""" 

if not os.path.isfile(default_filename):
    print ("File doesn't exist")
    exit(1)

try:
    """
        Initialize the spark service with the aws package
    """
    spark = SparkSession \
        .builder \
        .appName("Confitec Spark Test") \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
        .config('spark.hadoop.fs.s3a.access.key',envconfig['AWS_ACCESS_KEY_ID'] ) \
        .config('spark.hadoop.fs.s3a.secret.key', envconfig['AWS_SECRET_ACCESS_KEY']) \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()

    """ 
       Read the datasource
    """
    netflixFile=spark.read.parquet(default_filename)

    """ 
       Convert the Premier and dt_inclusao fields from string format to datetime
    """
    netflixFile = (
        netflixFile
        .withColumn("Premiere", unix_timestamp(col("Premiere"), "d-MMM-yy").cast("timestamp").cast(DateType()))
        .withColumn("dt_inclusao", col("dt_inclusao").cast("timestamp"))
    )

    """ 
        Removes all duplicated lines
    """
    netflixFile = netflixFile.dropDuplicates()
    
    #Order by Active(desc) and Genre fields
    netflixFile = netflixFile.sort(col("Active").desc(), col("Genre"))

    """       
        Translates all columns to Brazilian Portuguese
    """
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
    """
        Export the data to a csv file and send to AWS S3
    """
    netflixFile.select(col("Título"),col("Gênero"), col("Temporadas"), col("Pré-estreia"), col("Idioma"), col("Ativo"), col("Status"), col("Data de inclusão"), col("Data de Alteração")) \
        .repartition(1) \
        .write \
        .format("com.databricks.spark.csv") \
        .option("header", True) \
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") \
        .mode("overwrite") \
        .save("s3a://fintectest/%s" % defautl_path )
except AnalysisException as error:
    print("Failed to analyze a SQL query")
    print("Description %s: " % error)
    exit(1)
except IllegalArgumentException as error:
    print("Passed an illegal or inappropriate argument")
    print("Description %s: " % error)
    exit(1)
except ParseException as error:
    print("Failed to parse a SQL command")
    print("Description %s: " % error)
    exit(1)
except StreamingQueryException as error: 
    print("Exception that stopped a :class:`StreamingQuery`")
    print("Description %s: " % error)
    exit(1)
except QueryExecutionException as error: 
    print("Failed to execute a query.")
    print("Description %s: " % error)
    exit(1)
except PythonException as error:
    print("Exceptions thrown from Python workers")
    print("Description %s: " % error)
    exit(1)    
except UnknownException as error:
    print("UnknownException")
    print("Description %s: " % error)
    exit(1)    
    



print ("Checking bucket... ", end="")
print(get_last_csv_link(envconfig['AWS_DEFAULT_BUCKET'], defautl_path))
