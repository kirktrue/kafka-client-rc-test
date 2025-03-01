package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMessageConsumer implements Runnable {

    private final Consumer<String, String> consumer;
    private final String topic;
    private final CountDownLatch latch;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaMessageConsumer(String bootstrapServers,
                                String topic,
                                GroupProtocol groupProtocol,
                                CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-application-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());

        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.latch = latch;
    }

    @Override
    public void run() {
        consumer.subscribe(Set.of(topic));

        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(r -> latch.countDown());
                long count = latch.getCount();

                if (count % 100 == 0 || count < 10) {
                    System.out.println(count + " messages remaining");
                }

                if (count <= 0)
                    break;
            }
        } catch (WakeupException e) {
            // Ignore...
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
