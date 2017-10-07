package com.hlaing.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;

import com.hlaing.kafka.model.Greeting;

public class Receiver {
	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

	private CountDownLatch latch = new CountDownLatch(3);

	private CountDownLatch partitionLatch = new CountDownLatch(2);

	private CountDownLatch filterLatch = new CountDownLatch(2);

	private CountDownLatch greetingLatch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
		return latch;
	}

	public CountDownLatch getPartitionLatch() {
		return partitionLatch;
	}

	public CountDownLatch getFilterLatch() {
		return filterLatch;
	}

	public CountDownLatch getGreetingLatch() {
		return greetingLatch;
	}

	@KafkaListener(topics = "${message.topic.name}", group = "helloworld", containerFactory = "kafkaListenerContainerFactory")
	public void listenGroupFoo(String message) {
		System.out.println("Received Messasge in group 'helloworld': " + message);
		this.latch.countDown();
	}

	@KafkaListener(topics = "${message.topic.name}", group = "helloworld", containerFactory = "kafkaListenerContainerFactory")
	public void listenGroupBar(String message) {
		System.out.println("Received Messasge in group 'helloworld': " + message);
		latch.countDown();
	}

	@KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Messasge: " + message + " from partition: " + partition);
		latch.countDown();
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0", "3" }))
	public void listenToParition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Message: " + message + " from partition: " + partition);
		this.partitionLatch.countDown();
	}

	@KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenWithFilter(String message) {
		System.out.println("Recieved Message in filtered listener: " + message);
		this.filterLatch.countDown();
	}

	@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	public void greetingListener(Greeting greeting) {
		System.out.println("Recieved greeting message: " + greeting);
		this.greetingLatch.countDown();
	}
}
