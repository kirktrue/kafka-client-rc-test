package com.example;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaApplication {

    private static final String TOPIC = "test-topic";

    public static void main(String[] args) {
        // Create command line options
        Options options = createCommandLineOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            String brokerHost = cmd.getOptionValue("broker");
            int messageCount = Integer.parseInt(cmd.getOptionValue("count"));
            String acks = cmd.getOptionValue("acks");
            int messageSize = Integer.parseInt(cmd.getOptionValue("size"));

            System.out.println(
                "Starting Kafka application with the following configuration:"
            );
            System.out.println("Broker Host: " + brokerHost);
            System.out.println("Message Count: " + messageCount);
            System.out.println("Acks: " + acks);
            System.out.println("Message Size: " + messageSize);

            // CountDownLatch to wait for consumer to receive all messages
            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger receivedMessages = new AtomicInteger(0);

            // Create and start the consumer in a separate thread
            KafkaMessageConsumer consumer = new KafkaMessageConsumer(
                brokerHost,
                TOPIC,
                receivedMessages,
                latch,
                messageCount
            );
            Thread consumerThread = new Thread(consumer);
            consumerThread.start();

            // Create and use the producer
            KafkaMessageProducer producer = new KafkaMessageProducer(
                brokerHost,
                acks
            );
            for (int i = 0; i < messageCount; i++) {
                String message = generateRandomString(messageSize);
                producer.sendMessage(TOPIC, message);
                System.out.println("Sent message " + (i + 1) + ": " + message);
            }

            producer.close();

            try {
                // Wait for consumer to receive all messages
                latch.await();
                consumer.shutdown();
            } catch (InterruptedException e) {
                System.err.println(
                    "Application interrupted: " + e.getMessage()
                );
            }

            System.out.println(
                "Application completed successfully. Sent and verified " +
                    messageCount +
                    " messages."
            );
        } catch (ParseException e) {
            System.err.println(
                "Error parsing command line arguments: " + e.getMessage()
            );
            formatter.printHelp("KafkaApplication", options);
            System.exit(1);
        }
    }

    private static Options createCommandLineOptions() {
        Options options = new Options();

        Option brokerOption = Option.builder("b")
            .longOpt("broker")
            .desc("Kafka broker host name (e.g., localhost:9092)")
            .hasArg()
            .required(true)
            .build();

        Option countOption = Option.builder("c")
            .longOpt("count")
            .desc("Number of messages to send")
            .hasArg()
            .required(true)
            .type(Number.class)
            .build();

        Option acksOption = Option.builder("a")
            .longOpt("acks")
            .desc("Acknowledgment setting (e.g., '0', '1', or 'all')")
            .hasArg()
            .required(true)
            .build();

        Option sizeOption = Option.builder("s")
            .longOpt("size")
            .desc("Size of random message strings")
            .hasArg()
            .required(true)
            .type(Number.class)
            .build();

        options.addOption(brokerOption);
        options.addOption(countOption);
        options.addOption(acksOption);
        options.addOption(sizeOption);

        return options;
    }

    private static String generateRandomString(int length) {
        // Using Java 11 stream features to generate random string
        Random random = new Random();
        return random
            .ints(48, 123)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(
                StringBuilder::new,
                StringBuilder::appendCodePoint,
                StringBuilder::append
            )
            .toString();
    }
}
