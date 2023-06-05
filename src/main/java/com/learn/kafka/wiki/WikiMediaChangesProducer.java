package com.learn.kafka.wiki;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikiMediaChangesProducer {
	public static void main(String[] args) {
		System.out.println("hello");
		
		String bootstrapServer="192.168.55.11:9092";
		Properties properties=new Properties();
		properties.setProperty("bootstrap.servers", bootstrapServer);
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule");
		properties.setProperty("sasl.machanism", "PLAIN");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		
		String topic="wikimedia_recentchange";
		
		EventHandler event=new WikimediaChangeHandler(producer, topic);
		String url= "https://stream.wikimedia.org/v2/stream/recentchange";
		EventSource.Builder builder = new EventSource.Builder(event,URI.create(url));
		EventSource eventSource=builder.build();
		
		eventSource.start();
		
//		we produce for 10 min then block the program
		try {
			TimeUnit.MINUTES.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
