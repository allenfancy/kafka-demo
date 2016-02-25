package com.allen.kafka.producer.async;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ASyncProduce {

	public static void main(String[] args){
		long events = 10;
		Random random = new Random();
		
		Properties props = new Properties();
		//存放Kafka集群配置的地址和访问的端口
		props.put("metadata.broker.list", "192.168.1.136:19092,192.168.179.135:19092,192.168.179.136:19092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.allen.kafka.producer.partiton.SimplePartitioner");
		props.put("producer.type", "async");
		
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String,String> producer = new Producer<String,String>(config);
		
		for(long nEvents = 0;nEvents < events;nEvents++){
			long runtime = new Date().getTime();
			String ip = "192.168.1."+random.nextInt(255);
			String msg = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(runtime) + ",www.example.com," + ip;
			KeyedMessage<String,String> data = new KeyedMessage<String,String>("allenTest",ip,msg);
			producer.send(data);
			try{
				Thread.sleep(1000);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		
		producer.close();
		
	}
}
