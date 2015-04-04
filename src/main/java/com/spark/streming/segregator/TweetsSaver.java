package com.spark.streming.segregator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import scala.Tuple2;

import com.spark.streming.configuration.ConfigurationManager;

public class TweetsSaver implements Function<JavaPairRDD<String,String>, Void> 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String esIndex = null;

	public TweetsSaver(String index) 
	{
		esIndex = index;
	}

	public Void call(JavaPairRDD<String, String> pairRDD) throws Exception 
	{
		pairRDD.foreach(new VoidFunction<Tuple2<String,String>>() 
		{
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, String> tuple) throws Exception 
			{
				Client client = ConfigurationManager.getEsClient();
//				client.p
				ListenableActionFuture<IndexResponse> resp = client.prepareIndex().setIndex( esIndex ).setType( "tweet" ).setSource(tuple._2).execute();
				System.out.println("Put TO ELASTICSEARCH - - - - - - - " + tuple._2);
				
				IndexResponse indResp = resp.actionGet();
			}
		});
		
		
		return null;
	}

}
