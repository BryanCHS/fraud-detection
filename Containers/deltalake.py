import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from datetime import datetime


# Create Spark Session 
def create_spark_session():
    builder = (
        SparkSession.builder.appName("DeltaLake Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


# Bronze state
def bronze():
    
    spark = create_spark_session()
    df =  spark.read.csv( "Data/Transacciones_Prueba_Especialista_Prevencion_Fraude.xlsx", header = True, inferSchema = True)
                            df.write.format("delta").mode("overwrite").save("Data/bronze_table")


# Airflow DAG
default_args = {"owner":"airflow", "start_date": datetime(2025,1,1)}
dag = DAG("delta_medallion_architecture", default_args = default_args, schedule_interval="@daily",catchup=False)

# DAG tasks
task_bronze = PythonOperator( task_id="load_bronze", python_callable=bronze,dag=dag) 

# Order
task_bronze