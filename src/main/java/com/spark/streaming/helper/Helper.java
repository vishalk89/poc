package com.spark.streaming.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import com.google.gson.Gson;
import com.spark.streming.segregator.LocationSeggregator;
import com.spark.streming.segregator.TweetsSaver;

public class Helper 
{
	private static int count = 0;
	private static String accessToken = "1693674889-OoFs8EtnV13NGhY5WvC0wfPqoZRe7NG6XNhJW7j";
	private static String consumerSecret = "5g5pzrBPk9lOMfnm1A4uO9PFEkh9M68cQVFvwvE4cDpYKbsws0";
	private static String consumerKey = "MminXqyp4JoDTLO33NPX7Ci9F";
	private static String accessTokenSecret = "TtDCyfnO7VjOC1599nFo1MsMqiq9KCmDrEbf3FbdTquHN";
	private static int i = 1;
	
	public static Configuration getTwitterAuthConfig()
	{
		ConfigurationBuilder conf = new ConfigurationBuilder();
		conf.setOAuthAccessToken(accessToken);
		conf.setOAuthAccessTokenSecret(accessTokenSecret);
		conf.setOAuthConsumerKey(consumerKey);
		conf.setOAuthConsumerSecret(consumerSecret);
		
		return conf.build();
	}

	public static void processTweets(JavaDStream<Status> tweets) 
	{
		JavaDStream<String> tweetList = tweets.map( new Function<Status, String>() {

			public String call(Status tweet) throws Exception {
				String something = DataObjectFactory.getRawJSON( tweet );
				return something;
			}
		});
		// TODO : categorize tweets as per locations
		tweets.mapToPair( new PairFunction<Status, String, List<Status>>() {

			public Tuple2<String, List<Status>> call(Status status) throws Exception 
			{
				List<Status> statusList = new ArrayList<Status>();
				
				return null;
			}
		} );
	}
	 
	public static void saveTweets(JavaDStream<Status> tweets, JavaSparkContext jsc)
	{
		System.out.println("-------Count reached : " + count);
		tweets.foreach( new Function<JavaRDD<Status>, Void>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Status> tweetsRDD) throws Exception 
			{
//				tweet.foreach(new VoidFunction<Status>() {
//					
//					/**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//
//					public void call(Status tweet) throws Exception {
//						Gson gson = new Gson();
//						String tweetJson = gson.toJson( tweet );
//						System.out.println(" ---------:: " + tweetJson);
//						JavaEsSpark.saveToEs(arg0, arg1);
//					}
//				});
				JavaRDD<String> tweetJson = tweetsRDD.map( new Function<Status, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public String call(Status tweet) throws Exception {
						Gson gson = new Gson();
						count++;
						return gson.toJson( tweet );
					}
				});
				JavaEsSpark.saveJsonToEs( tweetJson , "indiasdaughter/tweets" );
				System.out.println("---------------- tweet put into ES -------------------" + count);
				return null;
			}
		});
	}
	
	public static void saveHotelTweets(JavaDStream<Status> tweets, JavaSparkContext jsc, String index)
	{// TODO: not complete yet
//		tweets.foreach( new TweetSegragator() );
		JavaPairDStream<String, String> pair = tweets.mapToPair( new LocationSeggregator() );
		
		pair.foreach( new TweetsSaver( index ));
	}
	
	public void createDictionary()
	{
		
	}
	
	public static JavaPairRDD<String,Map<String,Object>> getDataFromES(JavaSparkContext jsc, String index, String type/*, String id*/)
	{
		JavaPairRDD<String, Map<String, Object>> data = JavaEsSpark.esRDD(jsc, index + "/" + type);
		
		return data;
	}
	
}
