package com.allen.kafka.producer.sync;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SyncProduce {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long events = Long.MAX_VALUE;
		Random random = new Random();
		Properties props = new Properties();
		//存放Kafka集群配置的地址和访问的端口
		props.put("metadata.broker.list", "192.168.1.136:19092,192.168.179.135:19092,192.168.179.136:19092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.allen.kafka.producer.partiton.SimplePartitioner");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
        
		for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + random.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
			   //eventKey必须有（即使自己的分区算法不会用到这个key，也不能设为null或者""）,否者自己的分区算法根本得不到调用
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("allenTest", ip, msg);
			   												//			 eventTopic, eventKey, eventBody
               
               producer.send(data);
			   try {
                   Thread.sleep(1000);
               } catch (InterruptedException ie) {
               }
        }
        producer.close();
	}

}
