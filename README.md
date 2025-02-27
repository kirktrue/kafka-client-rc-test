# kafka-client-rc-test
Simple Apache Kafka client application to use in testing release candidates for Apache Kafka 

## Building the Application

You can build the application with commands like:

```bash
./gradlew clean jar
```

## Running the Application

You can now run the application with commands like:

```bash
java \
  -jar build/libs/kafka-client-rc-test-1.0-SNAPSHOT.jar \
  --broker localhost:9092 \
  --count 100 \
  --acks all \
  --size 32
```

## Command Line Options

The application supports these options:

| Short Form | Long Form | Description                    | Required |
|------------|-----------|--------------------------------|----------|
| -b         | --broker  | Kafka broker host name         | Yes      |
| -c         | --count   | Number of messages to send     | Yes      |
| -a         | --acks    | Acknowledgment setting         | Yes      |
| -s         | --size    | Size of random message strings | Yes      |
