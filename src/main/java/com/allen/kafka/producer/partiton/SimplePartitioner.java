package com.allen.kafka.producer.partiton;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
/***
 * 自定义分区算法
 * @author allen
 *
 */
public class SimplePartitioner  implements Partitioner{

	 public SimplePartitioner (VerifiableProperties props) {
		 
	 }
	 
	public int partition(Object key, int a_numPartitions) {
		// TODO Auto-generated method stub
		int partition = 0;
		String stringKey = (String) key;
		int offset = stringKey.lastIndexOf(".");
		if(offset > 0){
			partition = Integer.parseInt(stringKey.substring(offset+1))%a_numPartitions;
		}
		return partition;
	}

}
