import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class StatisticsConsumer {
    public static void main(String[] args) {
        // Set properties used to configure the consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "group1");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        // Define the consumer
        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the numbers topic
        consumer.subscribe(Arrays.asList("numbers"));

        // Variables to hold the sum and count of numbers, and sum of squares of numbers
        double sum = 0;
        double sumOfSquares = 0;
        int count = 0;

        while (true) {
            ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, Integer> record : records) {
                int value = record.value();
                sum += value;
                sumOfSquares += value * value;
                count++;

                // Calculate and print average and standard deviation
                if (count == 50) {
                    double average = sum / count;
                    double variance = sumOfSquares / count - average * average;
                    double standardDeviation = Math.sqrt(variance);

                    System.out.println("Average: " + average);
                    System.out.println("Standard Deviation: " + standardDeviation);

                    // Reset variables for the next batch of 50 numbers
                    sum = 0;
                    sumOfSquares = 0;
                    count = 0;
                }
            }
        }
    }
}
