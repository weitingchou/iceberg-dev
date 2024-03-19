package org.example;

import org.apache.spark.sql.SparkSession;

import org.apache.iceberg.catalog.Catalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;
import java.util.HashMap;

public class Main
{
    public static void main(String[] args)
    {
        // Create a REST Catalog
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "http://rest:8181");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://warehouse/wh");
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(S3FileIOProperties.ENDPOINT, "http://minio:9000");

        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("demo", properties);

        // Define schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "sepal.length", Types.FloatType.get()),
                Types.NestedField.required(2, "sepal.width", Types.FloatType.get()),
                Types.NestedField.required(3, "petal.length", Types.FloatType.get()),
                Types.NestedField.required(4, "petal.width", Types.FloatType.get()),
                Types.NestedField.required(5, "variety", Types.StringType.get())
        );

        // Create table
        Namespace namespace = Namespace.of("test");
        TableIdentifier name = TableIdentifier.of(namespace, "iris");
        catalog.createTable(name, schema);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java API Demo")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.demo.type", "rest")
//                .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
                .config("spark.sql.catalog.demo.uri", "http://rest:8181")
                .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.demo.warehouse", "s3s://warehouse/wh/")
                .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
                .config("spark.sql.defaultCatalog", "demo")
                .config("spark.eventLog.enabled", "true")
                .config("spark.eventLog.dir", "/home/iceberg/spark-events")
                .config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // Insert data
        String query = "INSERT INTO demo.test.iris "
                    + "VALUES "
                    + "(5.1, 3.5, 1.4, 0.2, 'Setosa'), "
                    + "(4.9, 3, 1.4, 0.2, 'Setosa'), "
                    + "(4.7, 3.2, 1.3, 0.2, 'Setosa')";

        spark.sql(query).show();
        spark.stop();
    }
}