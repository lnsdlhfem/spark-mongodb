package com.lnsdlhfem;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * MongoDB Connector for Apache Spark
 *
 * @ClassName: GettingStarted    
 * @author: lnsdlhfem
 * @date: 2017/10/19 10:18
 */
public final class GettingStarted {

    public static void main(final String[] args) throws InterruptedException {
    /* Create the SparkSession.
     * If config arguments are passed from the command line using --conf,
     * parse args for the values to set.
     */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                // 读数据库
                .config("spark.mongodb.input.uri", "mongodb://mongodb1/test.sparkCollection")
                // 写数据库
                .config("spark.mongodb.output.uri", "mongodb://mongodb1/test.sparkCollection")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // More application logic would go here...

        jsc.close();

    }

}
