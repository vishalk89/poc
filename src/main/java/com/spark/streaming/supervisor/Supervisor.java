package com.spark.streaming.supervisor;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.spark.streaming.helper.Helper;

public class Supervisor 
{
	public static void main(String[] args) 
	{
		SparkConf sc = new SparkConf();
		sc.setMaster("local[5]");
		sc.setAppName("Supervisor");
		sc.set("es.index.auto.create", "true");
		
		final JavaSparkContext jsc = new JavaSparkContext( sc );
		JavaPairRDD<String, Map<String, Object>> tweetsSamples = Helper.getDataFromES(jsc, "twitter", "tweet");
		tweetsSamples.foreach( new VoidFunction<Tuple2<String,Map<String,Object>>>() {
			
			public void call(Tuple2<String, Map<String, Object>> arg0) throws Exception {
				// TODO Auto-generated method stub
				
			}
		});
		
		
		
	}
}
