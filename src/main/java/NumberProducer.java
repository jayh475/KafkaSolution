import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.Random;

public class NumberProducer {
    public static void main(String[] args) {
        // Set properties used to configure the producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("auto.offset.reset", "earliest");

        // Define the producer
        Producer<String, Integer> producer = new KafkaProducer<>(properties);

        // Generate 50 random numbers and send to Kafka topic
        Random random = new Random();
        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<>("numbers", "Number", random.nextInt(100)));
        }

        producer.close();
    }
}

