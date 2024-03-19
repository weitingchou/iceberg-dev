#! /bin/bash

spark-submit --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark/jars/iceberg-aws-bundle-1.5.0.jar --class "org.example.Main" target/IcebergLoader-1.0-SNAPSHOT.jar
