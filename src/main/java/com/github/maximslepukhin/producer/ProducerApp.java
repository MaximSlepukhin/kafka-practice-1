package com.github.maximslepukhin.producer;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.maximslepukhin.model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

public class ProducerApp {
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public ProducerApp() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        this.producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, Message message) {
        try {
            String key = message.getId();
            String value = mapper.writeValueAsString(message);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
            RecordMetadata metadata = future.get();
            System.out.println("Sent: " + value + " to partition " + metadata.partition() +
                               " with offset " + metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        ProducerApp producerApp = new ProducerApp();
        for (int i = 1; i <= 50; i++) {
            Message message = new Message();
            message.setId(UUID.randomUUID().toString());
            message.setContent("message " + i);
            producerApp.sendMessage("my-topic", message);
            Thread.sleep(100);
        }
        producerApp.close();
    }
}