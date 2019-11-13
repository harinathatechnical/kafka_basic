package com.github.harinatha.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
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

            String topic = "first_topic";
            String value = "Hello World "+Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            final ProducerRecord<String, String> record = new
                    ProducerRecord<String, String>(topic,key, value);
             logger.info("Key: "+key);
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
            }).get();// block the .send to make it synchronous -- don't do this in production
        }
        producer.flush();
        //flush and close
        producer.close();
    }
}
