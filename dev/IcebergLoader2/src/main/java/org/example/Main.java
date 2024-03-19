package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;

import org.apache.iceberg.hive.HiveCatalog;
import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;

public class Main
{

    private static void createTable(String namespace, String tableName) {
        Configuration conf = new Configuration();
        conf.set("hive.metastore.uris", "thrift://hive-metastore:9083");
        conf.set("hive.metastore.warehouse.dir", "s3a://iceberg/");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://mariadb:3306/metastore_db");
        conf.set("javax.jdo.option.ConnectionUserName", "admin");
        conf.set("javax.jdo.option.ConnectionPassword", "admin");
        conf.set("fs.s3a.access.key", "admin");
        conf.set("fs.s3a.secret.key", "password");
        conf.set("fs.s3a.endpoint", "http://minio:9000");
        conf.set("fs.s3a.path.style.access", "true");

        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(conf);
        catalog.initialize("iceberg", new HashMap<>());

        Namespace ns = Namespace.of(namespace);
        if (!catalog.namespaceExists(ns)) {
            System.out.println("Creating namespace");
            catalog.createNamespace(ns);
        }

        Schema schema = new Schema(
                Types.NestedField.required(1, "sepal.length", Types.FloatType.get()),
                Types.NestedField.required(2, "sepal.width", Types.FloatType.get()),
                Types.NestedField.required(3, "petal.length", Types.FloatType.get()),
                Types.NestedField.required(4, "petal.width", Types.FloatType.get()),
                Types.NestedField.required(5, "variety", Types.StringType.get())
        );

        TableIdentifier table = TableIdentifier.of(ns, tableName);
        if (!catalog.tableExists(table)) {
            System.out.println("Creating table");
            catalog.createTable(table, schema);
        }
    }

    public static void main(String[] args)
    {
        createTable("test", "iris");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Iceberg Loader")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type", "hive")
                .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.hive.HiveCatalog")
                .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg.warehouse", "s3s://iceberg/")
                .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
                .config("spark.sql.defaultCatalog", "iceberg")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "/home/iceberg/spark-events")
                .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> irisData = spark.read().format("csv")
                .option("sep, ","")
                .option("header", "true")
                .load("/home/iceberg/data/iris.csv");

        try {
            irisData.writeTo("iceberg.test.iris").append();
        } catch (NoSuchTableException e) {
            System.out.println("No such table");
        }
        spark.stop();
    }
}