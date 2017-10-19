package com.lnsdlhfem;

import com.lnsdlhfem.config.SparkSessionConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

/**
 * 从mongodb中读取数据
 *
 * @ClassName: ReadFromMongoDB
 * @author: lnsdlhfem
 * @date: 2017/10/19 13:50
 */
public class ReadFromMongoDB {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkSessionConfig().getSparkSession().sparkContext());

        // 读取数据
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        System.out.println(rdd.count());
        System.out.println(rdd.first().toJson());

        jsc.close();
    }

}
