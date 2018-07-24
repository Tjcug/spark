package org.apache.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * locate org.apache.spark
 * Created by 79875 on 2017/10/24.
 * 删除wordCountResult路径
 * hdfs dfs -rm -r hdfs://ubuntu2:9000/user/root/sparkMigrate/wordCountResult
 * 提交jar包
 * spark-submit --class org.apache.spark.TextFileWordCount --master spark://ubuntu2:7077 sparkMigrateAnalysis-2.3.3-SNAPSHOT.jar /user/root/sparkMigrate/data /user/root/sparkMigrate/wordCountResult
 */
public class TextFileWordCount {
    public static void main(String[] args) {

        String textFile=args[0];
        String outputFile=args[1];
        SparkConf conf=new SparkConf().setAppName("TextFileWordCount");

        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> textFileRDD = sc.textFile(textFile);
        JavaRDD<String> wordsRDD = textFileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split).iterator();
            }
        });
        JavaPairRDD<String, Integer> wordsPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordcountRDD = wordsPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordcountRDD.saveAsTextFile(outputFile);

        sc.close();
    }
}
