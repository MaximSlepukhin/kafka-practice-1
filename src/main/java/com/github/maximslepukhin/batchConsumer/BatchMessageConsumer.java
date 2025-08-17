package com.github.maximslepukhin.batchConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.maximslepukhin.model.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class BatchMessageConsumer {
    private static final int BATCH_SIZE = 10;   // Максимальный размер батча, который обрабатываем за один раз
    private static final int MAX_BUFFER_SIZE = 1000;    // Максимальный размер внутреннего буфера. Если превышен, обрабатываем все сразу
    private static final long MAX_WAIT_MS = 2000;   // Максимальное время ожидания до обработки неполного батча (в миллисекундах)

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");   // Отключаем автоматическую фиксацию оффсетов
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);    // Минимальный объем данных, который сервер будет ждать перед отправкой ответа
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);    // Максимальное время ожидания на сервере для формирования пакета данных (в мс)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500); // Максимальное количество записей, которое вернёт один poll

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));
        ObjectMapper mapper = new ObjectMapper();

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        long lastPollTime = System.currentTimeMillis();

        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);

                    if (buffer.size() >= BATCH_SIZE) {
                        processAndCommit(new ArrayList<>(buffer.subList(0, BATCH_SIZE)), consumer, mapper);
                        buffer.subList(0, BATCH_SIZE).clear();
                    }

                    // Защита от переполнения буфера. Если слишком много сообщений, обрабатываем все
                    if (buffer.size() > MAX_BUFFER_SIZE) {
                        System.err.println("Buffer exceeded MAX_BUFFER_SIZE, processing all messages");
                        processAndCommit(new ArrayList<>(buffer), consumer, mapper);
                        buffer.clear();
                    }
                }

                long now = System.currentTimeMillis();
                // Обрабатываем неполный батч, если прошло достаточно времени с последнего poll
                if (!buffer.isEmpty() && (now - lastPollTime >= MAX_WAIT_MS)) {
                    processAndCommit(new ArrayList<>(buffer), consumer, mapper);
                    buffer.clear();
                }

                if (!records.isEmpty()) {
                    lastPollTime = System.currentTimeMillis();
                }

            } catch (Exception e) {
                System.err.println("Error processing records: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void processAndCommit(List<ConsumerRecord<String, String>> buffer,
                                         KafkaConsumer<String, String> consumer,
                                         ObjectMapper mapper) {
        for (ConsumerRecord<String, String> record : buffer) {
            try {
                Message message = mapper.readValue(record.value(), Message.class);
                System.out.println("Received (Batch): " + message);
            } catch (Exception e) {
                System.err.println("Failed to deserialize message: " + record.value());
                e.printStackTrace();
            }
        }
        try {
            // Синхронная фиксация оффсетов, чтобы Kafka знала, что сообщения обработаны
            consumer.commitSync();
        } catch (Exception e) {
            System.err.println("Failed to commit offsets: " + e.getMessage());
            e.printStackTrace();
        }
    }
}