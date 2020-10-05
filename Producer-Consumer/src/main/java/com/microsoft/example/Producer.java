package com.microsoft.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class Producer
{
    public static void produce(String brokers, String topicName, String[] keys, String[] values, String eventIDPrefix) throws IOException
    {

        // Set properties used to configure the producer
        Properties properties = new Properties();
        // Set the brokers (bootstrap servers)
        properties.setProperty("bootstrap.servers", brokers);
        // Set how to serialize key/value pairs
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        // specify the protocol for Domain Joined clusters
        //properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // So we can generate random sentences
        Random random = new Random();

        String progressAnimation = "|/-\\";
        // Produce a bunch of records
        for(int i = 0; i < 100; i++) {
            // Pick a sentence at random
            String key = keys[random.nextInt(keys.length)];
            String content = values[random.nextInt(values.length)];

            JSONObject rankObj = new JSONObject();
            rankObj.append("EventId", eventIDPrefix + i);
            rankObj.append("content", content);

            // Send the sentence to the test topic
            try
            {
                // POC: TODO: how to use partitions
                producer.send(new ProducerRecord<String, String>(topicName, key, rankObj.toString())).get();
            }
            catch (Exception ex)
            {
                System.out.print(ex.getMessage());
                throw new IOException(ex.toString());
            }
            String progressBar = "\r" + progressAnimation.charAt(i % progressAnimation.length()) + " " + i;
            System.out.write(progressBar.getBytes());
        }
    }
}
