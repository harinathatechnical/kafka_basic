package com.github.harinatha.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.omg.SendingContext.RunTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {
       new ConsumerDemoWithThread().run();
           }
private ConsumerDemoWithThread(){}

           private void run(){
               Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

               String bootstrapServers = "127.0.0.1:9092";
               String groupId = "my-sixth-application";
               String topic = "first_topic";
               CountDownLatch latch = new CountDownLatch(1);
               Runnable myConsumerThread =  new ConsumerThread(
                       bootstrapServers,
                       groupId,
                       topic,
                       latch
               );

               Thread myThread = new Thread(myConsumerThread);
               myThread.start();

               Runtime.getRuntime().addShutdownHook(new Thread( ()->{
                           logger.info("caught Shutdown");
               ((ConsumerThread) myConsumerThread).shutdown();
                       }
               ));
               try {
                   latch.await();
               } catch (InterruptedException e) {
                   logger.info("Application got interrupted");
               }
               finally {
                   logger.info("Application is closing");
               }
           }
    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        public ConsumerThread(
                              String bootstrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch latch){
            this.latch=latch;
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            // create consumer
            KafkaConsumer<String,String> consumer =
                    new KafkaConsumer<String, String>(properties);
            //subscribe consumer to our topic
            consumer.subscribe(Arrays.asList(topic));
        }


        @Override
        public void run() {
           try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String,String> record : records){
                    logger.info("Key: "+record.key() +", Value : "+record.value());
                    logger.info("Partition: "+record.partition()+" ,Offset: "+record.offset());

                }
            }
           }catch(WakeupException e){
                logger.info("Received ShutDown Signal");
            }
           finally {
               consumer.close();
               //tell main thread we are done with consumer
               latch.countDown();
           }
        }

        public void shutdown(){
            consumer.wakeup();
        }
    }
}
