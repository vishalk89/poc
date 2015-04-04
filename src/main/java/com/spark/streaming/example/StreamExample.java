package com.spark.streaming.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;

import com.spark.streaming.helper.Helper;

public class StreamExample 
{
	public static void main(String[] args) 
	{
		Configuration twtConf = Helper.getTwitterAuthConfig();
		String[] searchFor = new String[]{"hotel"};

		SparkConf sc = new SparkConf().setAppName("Twitter_Streaming").setMaster("local[5]");
		sc.set("es.index.auto.create", "true");
		JavaStreamingContext jsc = new JavaStreamingContext( sc, Durations.seconds(1) );
		JavaReceiverInputDStream<Status> twtStream = TwitterUtils.createStream(jsc, new OAuthAuthorization( twtConf ), searchFor);
//		twtStream.print();
//		Helper.saveTweets( twtStream.cache(), jsc.sc() );
		
		Helper.saveHotelTweets(twtStream.cache(), jsc.sc(), "hotel");

		
		jsc.start();
		jsc.awaitTermination();
//		Helper.processTweets( twtStream.cache() );
	}
}
