package com.example.kafkaapp;

import com.example.consumers.ConsumerService;
import com.example.service.KafkaService;

public class TDD {

	public static void main(String[] args) {
		KafkaService kafkaService = new KafkaService();
		kafkaService.createTopic();
		ConsumerService consumerService=new ConsumerService();
		consumerService.subscribe("test");
	}

}
