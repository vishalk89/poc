package com.spark.streming.segregator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import twitter4j.Status;


public class TweetSegragator implements Function<JavaRDD<Status>, Void>
{

	public Void call(JavaRDD<Status> tweets) throws Exception 
	{
		Gson gson = new Gson();
		JavaRDD<String> json = tweets.map( new Function<Status, String>() 
				{
					public String call(Status status) throws Exception 
					{
						Gson gson = new Gson();
						
						return gson.toJson( status );
					}
				});
		
		return null;
	}

}
