package com.lnsdlhfem;

import com.lnsdlhfem.config.SparkSessionConfig;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 将spark数据写入mongodb
 *
 * @ClassName: WriteToMongoDB
 * @author: lnsdlhfem
 * @date: 2017/10/19 10:31
 */
public class WriteToMongoDB {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext(new SparkSessionConfig().getSparkSession().sparkContext());

        // spark RDD要存入mongodb，必须先转换为BsonDocument或DBObject
        // Create a RDD of 10 documents
        JavaRDD<Document> documentJavaRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map(
                new Function<Integer, Document>() {
                    public Document call(Integer i) throws Exception {
                        return Document.parse("{test: " + i + "}");
                    }
                }
        );

        // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("uri", "mongodb://admin:111111@mongodb1:27017");
        writeOverrides.put("database", "test");
        writeOverrides.put("collection", "sparkCollection");

        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        MongoSpark.save(documentJavaRDD);

        jsc.close();
    }
}
