package com.github.maximslepukhin.singleConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.maximslepukhin.model.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SingleMessageConsumer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");    // Включаем автоматическую фиксацию оффсетов
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");   // Интервал автокоммита (в миллисекундах)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Если оффсет не найден (например, новая группа), начинаем чтение с самого начала топика

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));
        ObjectMapper mapper = new ObjectMapper();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Message message = mapper.readValue(record.value(), Message.class);
                        System.out.println("Received (Single): " + message);
                    } catch (Exception e) {
                        System.err.println("ERROR: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}