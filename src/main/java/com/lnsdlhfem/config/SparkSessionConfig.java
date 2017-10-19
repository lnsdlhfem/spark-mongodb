package com.lnsdlhfem.config;

import org.apache.spark.sql.SparkSession;

public final class SparkSessionConfig {

    private SparkSession sparkSession;

    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                // 读数据库
                .config("spark.mongodb.input.uri", "mongodb://admin:111111@mongodb1/test.sparkCollection" +
                        "?authSource=admin&readPreference=primaryPreferred")
                // 写数据库
                .config("spark.mongodb.output.uri", "mongodb://admin:111111@mongodb1/test.sparkCollection" +
                        "?authSource=admin&readPreference=primaryPreferred")
                .getOrCreate();

        return spark;
    }
}
