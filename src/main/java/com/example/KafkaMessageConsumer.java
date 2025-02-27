package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaMessageConsumer implements Runnable {

    private final Consumer<String, String> consumer;
    private final String topic;
    private final AtomicInteger messageCounter;
    private final CountDownLatch latch;
    private final int expectedMessageCount;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaMessageConsumer(
        String bootstrapServers,
        String topic,
        AtomicInteger messageCounter,
        CountDownLatch latch,
        int expectedMessageCount
    ) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-application-consumer");
        props.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );
        props.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName()
        );
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");

        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.messageCounter = messageCounter;
        this.latch = latch;
        this.expectedMessageCount = expectedMessageCount;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(
                    Duration.ofMillis(100)
                );

                for (ConsumerRecord<String, String> record : records) {
                    int current = messageCounter.incrementAndGet();
                    System.out.println(
                        "Received message " + current + ": " + record.value()
                    );

                    if (current >= expectedMessageCount) {
                        latch.countDown();
                        return;
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
