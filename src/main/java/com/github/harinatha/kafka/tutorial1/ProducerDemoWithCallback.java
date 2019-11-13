package com.github.harinatha.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
       // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create the Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        for(int i=0;i<10;i++) {
            final ProducerRecord<String, String> record = new
                    ProducerRecord<String, String>("first_topic", "hello world"+Integer.toString(i));

            //send data

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is success or exception
                    if (e == null) {
                        //record was success sent
                        logger.info("Received new metadata \n" +
                                "Topic :" + recordMetadata.topic() + " \n" +
                                "Partition:" + recordMetadata.partition() + " \n " +
                                "Offset: " + recordMetadata.offset() + " \n " +
                                "TimeStamp: " + recordMetadata.timestamp());

                    } else {
                        logger.error("Error While producing", e);

                    }
                }
            });
        }
        producer.flush();
        //flush and close
        producer.close();
    }
}
