package com.wl.streaming.kafka.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","192.168.0.127:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("batch.size","10");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i =0; i<10;i++){
            producer.send(new ProducerRecord<String, String>("test-group",
                    Integer.toString(i),Integer.toString(i)));
            System.out.println("第"+i+"条消息");
        }

        producer.close();
    }
}
