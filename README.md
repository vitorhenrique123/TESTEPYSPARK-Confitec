# TESTEPYSPARK-Confitec 

## Quick Start

### Project dependencies
In order to use the application, you will first need:
| Software        | Description           | Download  |
| ------------- |:-------------:| -----:|
| Python (3.8.5)     | core technology for running the test | https://www.python.org/ |
| Spark 3.1.1 with Hadoop 2.7      | Spark™ is a unified analytics engine for large-scale data processing.      |   https://www.apache.org/dyn/closer.lua/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz |
| Hadoop AWS Package 2.7.3  | this module contains code to support integration with Amazon Web Services      |    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar |
|  Virtualenv (optional)  | tool to create isolated Python environments.      |    https://pypi.org/project/virtualenv/ |
## Installing 

You need to install the project dependencies

1. Clone this repo

2. Check this Requirements 
    1. Install Spark + Hadoop [Example](https://www.liquidweb.com/kb/how-to-install-apache-spark-on-ubuntu/)
    2. export $SPARK_HOME on you're bash environment
    3. Download the hadoop-aws-2.7.3.jar on $SPARK_HOME/jars

2. Install all the dependencies (Any requirements that install locally with the following command)

```bash
$ pip install -r requirements.txt
```
## Create .env with AWS Credentials 
1. Variables
    ```bash
        $ AWS_ACCESS_KEY_ID     =''
        $ AWS_SECRET_ACCESS_KEY =''
        $ AWS_DEFAULT_REGION    =''
        $ AWS_DEFAULT_BUCKET    =''
    ```  
## Ready to run
```bash
$ python testpyspark.py
```

## Project Structure
```
/
|   requirements.txt - all python depedencies
|   README.md - current file
|   s3_tools.py - s3 boto3 connection to get uploaded csv file
|   testpyspark.py - the main script file responsible for handling the datasource
|   OriginaisNetflix.parquet - datasource
```
