package com.spark.streming.segregator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import twitter4j.Status;

import com.google.gson.Gson;

public class LocationSeggregator implements PairFunction<Status, String, String> 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Tuple2<String, String> call(Status userStatus) throws Exception 
	{
		String location = userStatus.getUser().getLocation();
		Gson gson = new Gson();
		return new Tuple2<String, String>(location, gson.toJson( userStatus ));
	}

}
