package com.lnsdlhfem;

import com.lnsdlhfem.config.SparkSessionConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.util.Collections;

/**
 * mongodb聚合
 *
 * @ClassName: Aggregation
 * @author: lnsdlhfem
 * @date: 2017/10/19 13:57
 */
public class Aggregation {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkSessionConfig().getSparkSession().sparkContext());

        // 读取数据
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> aggregationRdd = rdd.withPipeline(
                Collections.singletonList(Document.parse("{ $match: { test : { $gt : 5 } } }"))
        );

        System.out.println(aggregationRdd.count());
        System.out.println(aggregationRdd.first().toJson());

        jsc.close();
    }
}
