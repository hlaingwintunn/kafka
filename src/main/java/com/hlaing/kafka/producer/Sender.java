package com.hlaing.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import com.hlaing.kafka.model.Greeting;

public class Sender {
	private static final Logger logger = LoggerFactory.getLogger(Sender.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaTemplate<String, Greeting> greetingKafkatemplate;

	@Value(value = "${message.topic.name}")
	private String topicName;

	@Value(value = "${partitioned.topic.name}")
	private String partionedTopicName;

	@Value(value = "${filtered.topic.name}")
	private String filteredTopicName;

	@Value(value = "${greeting.topic.name}")
	private String greetingTopicName;

	public void sendMessage(String message) {
		kafkaTemplate.send(topicName, message);
	}

	public void sendMessageToPartion(String message, int partition) {
		kafkaTemplate.send(partionedTopicName, partition, message);
	}

	public void sendMessageToFiltered(String message) {
		kafkaTemplate.send(filteredTopicName, message);
	}

	public void sendGreetingMessage(Greeting greeting) {
		greetingKafkatemplate.send(greetingTopicName, greeting);
	}

}
