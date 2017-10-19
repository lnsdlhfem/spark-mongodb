package com.lnsdlhfem;

import com.lnsdlhfem.config.SparkSessionConfig;
import com.lnsdlhfem.entity.Character;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class DatasetSQLDemo {

    /**
     test collection
     { "_id" : ObjectId("585024d558bef808ed84fc3e"), "name" : "Bilbo Baggins", "age" : 50 }
     { "_id" : ObjectId("585024d558bef808ed84fc3f"), "name" : "Gandalf", "age" : 1000 }
     { "_id" : ObjectId("585024d558bef808ed84fc40"), "name" : "Thorin", "age" : 195 }
     { "_id" : ObjectId("585024d558bef808ed84fc41"), "name" : "Balin", "age" : 178 }
     { "_id" : ObjectId("585024d558bef808ed84fc42"), "name" : "Kíli", "age" : 77 }
     { "_id" : ObjectId("585024d558bef808ed84fc43"), "name" : "Dwalin", "age" : 169 }
     { "_id" : ObjectId("585024d558bef808ed84fc44"), "name" : "Óin", "age" : 167 }
     { "_id" : ObjectId("585024d558bef808ed84fc45"), "name" : "Glóin", "age" : 158 }
     { "_id" : ObjectId("585024d558bef808ed84fc46"), "name" : "Fíli", "age" : 82 }
     { "_id" : ObjectId("585024d558bef808ed84fc47"), "name" : "Bombur" }

     */

    public static void main(String[] args) {
        SparkSession sparkSession = new SparkSessionConfig().getSparkSession();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "character");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> implicitDS = MongoSpark.load(jsc, readConfig).toDF();
        implicitDS.printSchema();
        implicitDS.show();

        // Load data with explicit schema
        Dataset<Character> explicitDS = MongoSpark.load(jsc, readConfig).toDS(Character.class);
        explicitDS.printSchema();
        explicitDS.show();

        // Create the temp view and execute the query
        explicitDS.createOrReplaceTempView("characters");
        Dataset<Row> centenarians = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100");
        centenarians.show();

        // Write the data to the "hundredClub" collection
        MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

        // Load the data from the "hundredClub" collection
        MongoSpark.load(sparkSession, ReadConfig.create(sparkSession).withOption("collection", "hundredClub"), Character.class).show();

        jsc.close();
    }
}
