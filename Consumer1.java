import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	public static void main(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.out.println("the arguments should be two");
			System.exit(-1);
		}
		String topicName = args[0].toString();
		String groupId = args[1].toString();
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092");
        	props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        	props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        	props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        	props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        	KafkaConsumer kafkaConsumer = new KafkaConsumer<String,String>(props);
        	kafkaConsumer.subscribe(Arrays.asList(topicName));
        	while(true)
        	{
        		ConsumerRecords<String,String> records = kafkaConsumer.poll(100);
        		for(ConsumerRecord<String,String> record : records)
        		{
        			System.out.println(record.value());
        		}
        	}
	}

}

