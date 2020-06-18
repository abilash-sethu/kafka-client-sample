package com.example.consumers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerService {

	private ExecutorService executorService = Executors.newFixedThreadPool(20);

	public void subscribe(String topic) {

		executorService.execute(() -> {
			System.out.println("Thread has been started");
			Map<String, Object> configs = new HashMap<String, Object>();
			configs.put("bootstrap.servers", "localhost:9092");
			configs.put("group.id","mygroup");
			configs.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
			configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
			consumer.subscribe(Collections.singletonList(topic));
			boolean exitLoop = false;
			while (!exitLoop) {
				try {
					System.out.println("consumer started polling from topic " + topic);
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
					records.forEach(record -> {
						System.out.println("Record consumed " + record);
					});

				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("An error occured during the polling the topic shutting down the consumer");
					exitLoop = true;
				}
			}
			consumer.close();
		});

	}

}
