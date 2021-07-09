package com.rodolfobandeira.score;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());

        /*
            Since this is a Log Service, we want to interact to all topics and events.
            Here is an interesting thing. We can create a subscriber using regex
            and this way, the consumer will subscribe for both our topics:
              - NEW_SCORE
              - NEW_SCORE_PUSH_NOTIFICATION

            All this thanks to: Pattern.compile(REGEX)
         */
        consumer.subscribe(Pattern.compile("NEW_SCORE.*"));

        while(true) {
            var records = consumer.poll(Duration.ofMillis(5000));

            if (records.isEmpty()) {
                System.out.println("No records found");
            } else {

                for (var record : records) {
                    System.out.println("Processing new score LOG");
                    System.out.println("Key: " + record.key());
                    System.out.println("Value: " + record.value());
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Offset: " + record.offset() + " \n\n");
                    // Here we would place our logic to store the logs into a database


                    // The following is just for debug purpose
                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Score LOG processed");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Changing here from "ScoreConsumer.class" to "LogService.class" makes possible that
        the first ScoreConsumer process the entry but also allows the LogService to process as well.

        If both consumers have the same GROUP_ID_CONFIG, the processing becomes atomic as soon as the first
        consumer process something, it doesn't allow another "job" process the same item.
         */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }
}