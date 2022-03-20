package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String [] args){
        log.info("I'm a Kafka Producer!");

        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int i = 0 ; i<10; i++){

            var topic = "demo_java";
            var value = "hello world " + i;
            var key = "id_" + i;
            // create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java","Hello World");

            // Send the data - Asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes everytime a record is sucessfully sent or an exception is thrown
                    if( e == null){
                        //record sucessfully sent
                        log.info("Received new metadata / \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Key : " + producerRecord.key() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n"

                        );
                    }else{
                        log.error("Error while producing: ", e.getMessage());
                    }
                }
            });

        }

        // Flush Data - Synchronous
        producer.flush();

        // Flush and close producer
        producer.close();

    }
}
