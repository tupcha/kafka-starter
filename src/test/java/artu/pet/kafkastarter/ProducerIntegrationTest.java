package artu.pet.kafkastarter;

import artu.pet.kafkastarter.data.KafkaTestMessage;
import artu.pet.kafkastarter.props.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class ProducerIntegrationTest {
    private static final String TOPIC = "test_data";

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("apache/kafka:latest"));

    @BeforeAll
    static void setup() {
        kafkaContainer.start();
    }

    @AfterAll
    static void tearDown() {
        kafkaContainer.close();
    }

    private KafkaProducer<String, byte[]> createProducer(Properties props) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<>(props);
    }



    @Test
    @DisplayName("Producer with default configuration should send messages successfully")
    public void testProducerWithDefaultConfig() throws Exception {
        Properties props = new Properties();
        KafkaProducer<String, byte[]> producer = createProducer(props);

        KafkaTestMessage message = KafkaTestMessage.builder()
                .id("test-id-1")
                .content("Default config test")
                .timestamp(System.currentTimeMillis())
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.id(), message.content().getBytes());

        Future<?> future = producer.send(record);
        producer.flush();

        assertThat(future.get()).isNotNull(); // Проверяем успешную отправку
        producer.close();
    }

    @Test
    @DisplayName("Producer with idempotence enabled should not duplicate messages")
    public void testProducerWithIdempotenceEnabled() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Включаем идемпотентность
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Требуется для идемпотентности
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Ограничение для идемпотентности
        KafkaProducer<String, byte[]> producer = createProducer(props);


        KafkaTestMessage message = KafkaTestMessage.builder()
                .id("test-id-2")
                .content("Idempotent test")
                .timestamp(System.currentTimeMillis())
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.id(), message.content().getBytes());

        Future<?> future = producer.send(record);
        producer.send(record); // Отправляем тот же ключ ещё раз
        producer.flush();

        assertThat(future.get()).isNotNull(); // Успешная отправка с идемпотентностью
        producer.close();
    }

    @Test
    @DisplayName("Producer with acks=0 should not wait for acknowledgment")
    public void testProducerWithAcksZero() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // Без подтверждений
        KafkaProducer<String, byte[]> producer = createProducer(props);

        KafkaTestMessage message = KafkaTestMessage.builder()
                .id("test-id-3")
                .content("Acks=0 test")
                .timestamp(System.currentTimeMillis())
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.id(), message.content().getBytes());

        Future<?> future = producer.send(record);
        producer.flush();

        assertThat(future.get()).isNotNull(); // Отправка без ожидания подтверждения
        producer.close();
    }

    @Test
    @DisplayName("Producer with retries should retry on failure")
    public void testProducerWithRetries() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.RETRIES_CONFIG, "3"); // 3 попытки
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100"); // Задержка между попытками 100 мс
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500"); // Уменьшаем таймаут запроса до 500 мс
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1000"); // Таймаут доставки 1000 мс (должен быть >= linger.ms + request.timeout.ms)
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // Явно задаём linger.ms для ясности
        KafkaProducer<String, byte[]> producer = createProducer(props);


        KafkaTestMessage message = KafkaTestMessage.builder()
                .id("test-id-4")
                .content("Retries test")
                .timestamp(System.currentTimeMillis())
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.id(), message.content().getBytes());

        Future<?> future = producer.send(record);
        producer.flush();

        assertThat(future.get()).isNotNull(); // Успешная отправка с ретраями
        producer.close();
    }

    @Test
    @DisplayName("Producer with very short timeout should throw exception")
    public void testProducerWithShortTimeout() {
        Properties props = new Properties();
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1"); // Очень короткий таймаут
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1"); // Очень короткий таймаут доставки
        KafkaProducer<String, byte[]> producer = createProducer(props);

        KafkaTestMessage message = KafkaTestMessage.builder()
                .id("test-id-5")
                .content("Short timeout test")
                .timestamp(System.currentTimeMillis())
                .build();

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, message.id(), message.content().getBytes());

        Future<?> future = producer.send(record);

        // Ожидаем исключение из-за короткого таймаута
        assertThrows(ExecutionException.class, future::get, "Expected timeout exception due to short timeout");
        assertThat(future.isDone()).isTrue();

        producer.close();
    }
}
