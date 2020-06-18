package com.example.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

public class KafkaService {

	public void createTopic() {
		System.out.println("topic is creating..");
		Properties properties = new Properties();
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		AdminClient admin = AdminClient.create(properties);
		Map<String, String> configs = new HashMap<String, String>();
		int partitions = 1;
		short replication = 1;
		NewTopic newTopics = new NewTopic("mock", partitions, replication).configs(configs);
		CreateTopicsResult result = admin.createTopics(Arrays.asList(newTopics));
		KafkaFuture<Void> resultFuture = result.all();
		try {
			Thread.sleep(10000l);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(resultFuture.isDone()) {
			System.out.println("topics is successfully created... ");
		}else {
			System.out.println("couldn't create the topic ");
		}
	}

}
