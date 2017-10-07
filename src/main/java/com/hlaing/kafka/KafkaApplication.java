package com.hlaing.kafka;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.hlaing.kafka.consumer.Receiver;
import com.hlaing.kafka.model.Greeting;
import com.hlaing.kafka.producer.Sender;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) throws InterruptedException {
		ConfigurableApplicationContext  context = SpringApplication.run(KafkaApplication.class, args);
		
		Sender messageSender = context.getBean(Sender.class);
		
		Receiver messageReceiver = context.getBean(Receiver.class);
		
		messageSender.sendMessage("HELLO WORLD!");
		messageReceiver.getLatch().await(10, TimeUnit.SECONDS);
		
		/*
         * Sending message to a topic with 5 partition,
         * each message to a different partition. But as per
         * listener configuration, only the messages from
         * partition 0 and 3 will be consumed.
         */
        for (int i = 0; i < 5; i++) {
        	messageSender.sendMessageToPartion("Hello To Partioned Topic!", i);
        }
        messageReceiver.getPartitionLatch().await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'filtered' topic. As per listener
         * configuration,  all messages with char sequence
         * 'World' will be discarded.
         */
        messageSender.sendMessageToFiltered("Hello Baeldung!");
        messageSender.sendMessageToFiltered("Hello World!");
        messageReceiver.getFilterLatch().await(10, TimeUnit.SECONDS);

        /*
         * Sending message to 'greeting' topic. This will send
         * and recieved a java object with the help of 
         * greetingKafkaListenerContainerFactory.
         */
        messageSender.sendGreetingMessage(new Greeting("Greetings", "World!"));
        messageReceiver.getGreetingLatch().await(10, TimeUnit.SECONDS);
		
		context.close();
	}
	
	@Bean
	public Sender send() {
		return new Sender();
	}
	
	@Bean
	public Receiver receive() {
		return new Receiver();
	}
}
