package com.allen.kafka.consumer.group;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class GroupConsumerTest extends Thread {

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public GroupConsumerTest(String a_zookeeper, String a_groupId, String a_topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));
		this.topic = a_topic;
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();

		try {
			if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down,exiting uncleanly");
			}

		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(a_numThreads);
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Please assign partition number.");
		}
		String zookeeper = "192.168.1.136:21181,192.168.179.135:21181,192.168.179.136:21181";

		String groupId = "allenGroupTest";
		String topic = "allenTest";
		int threads = Integer.parseInt(args[0]);

		GroupConsumerTest example = new GroupConsumerTest(zookeeper, groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException ie) {

		}
		example.shutdown();
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

}
