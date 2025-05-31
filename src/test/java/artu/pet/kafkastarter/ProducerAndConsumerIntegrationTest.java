package artu.pet.kafkastarter;

import artu.pet.kafkastarter.data.KafkaTestMessage;
import artu.pet.kafkastarter.exception.TestException;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ProducerAndConsumerIntegrationTest {
    private static final String TEST_TOPIC = "some_topic_test";

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka:latest"));
    @BeforeAll
    static void setup() {
        kafkaContainer.start();
    }

    @AfterAll
    static void tearDown() {
        kafkaContainer.close();
    }
    private KafkaProducer<String, byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }


    @Test
    @SneakyThrows(TestException.class)
    public void tesProducerAndConsumer() {
        KafkaProducer<String, byte[]> producer = createProducer();
        KafkaConsumer<String, byte[]> consumer = createConsumer();

        consumer.subscribe(Collections.singleton(TEST_TOPIC));

        KafkaTestMessage kafkaEvent = KafkaTestMessage.builder()
                .id("test-id 1")
                .content("Test message from test")
                .timestamp(System.currentTimeMillis())
                .build();


        ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(TEST_TOPIC, kafkaEvent.id(), kafkaEvent.content().getBytes());
        producer.send(kafkaRecord);
        producer.flush();


        consumer.poll(Duration.ofSeconds(10))
                .forEach(record -> {
                    if (record.topic().equals(TEST_TOPIC)) {
                        String key = record.key();
                        String value = new String(record.value());
                        System.out.println("Received message with key: " + key + ", value: " + value);
                        assertThat(key).isEqualTo(kafkaEvent.id());
                        assertThat(value).isEqualTo(kafkaEvent.content());
                        assertThat(record.topic()).isEqualTo(TEST_TOPIC);
                        if (!key.equals(kafkaEvent.id()) || !value.equals(kafkaEvent.content())) {
                            throw new TestException("Received message does not match sent message");
                        }
                    }
                });
    }
}
