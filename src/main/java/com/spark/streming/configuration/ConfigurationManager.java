package com.spark.streming.configuration;

import java.io.Serializable;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeBuilder;

public class ConfigurationManager implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Client esClient = NodeBuilder.nodeBuilder().node().client();
	
//	private ConfigurationManager() 
//	{
//		
//		esClient = NodeBuilder.nodeBuilder().node().client();
//	}
	
	public static Client getEsClient()
	{
		return esClient;
	}
}
