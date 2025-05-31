package artu.pet.kafkastarter.autoconfiguration;

import artu.pet.kafkastarter.props.KafkaProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnClass({KafkaTemplate.class, ConcurrentKafkaListenerContainerFactory.class})
@EnableKafka
@EnableConfigurationProperties(value = {KafkaProperties.class})
@ConditionalOnProperty(prefix = "custom.kafka",name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class CustomKafkaAutoConfiguration {

    private final KafkaProperties properties;

    // Producer Configuration
    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, byte[]> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, properties.getProducer().isIdempotenceEnabled());
        config.put(ProducerConfig.ACKS_CONFIG, String.valueOf(properties.getProducer().getAcks()));
        config.put(ProducerConfig.RETRIES_CONFIG, properties.getProducer().getRetries());
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, properties.getProducer().getRetryBackoffMs());
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, properties.getProducer().getDeliveryTimeoutMs());
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, properties.getProducer().getRequestTimeoutMs());
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, properties.getProducer().getMaxInFlightRequests());
        config.putAll(properties.getProducer().getConfig());
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> producerFactory) {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(properties.getProducer().getTopic());
        return template;
    }

    // Consumer Configuration
    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory<String, byte[]> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getConsumer().getMaxPollRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, properties.getConsumer().getMaxPollIntervalMs());
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getConsumer().getSessionTimeoutMs());
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, properties.getConsumer().getHeartbeatIntervalMs());
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, properties.getConsumer().getFetchMaxBytes());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.getConsumer().isAutoCommitEnabled());
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, properties.getConsumer().getAutoCommitIntervalMs());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        config.putAll(properties.getConsumer().getConfig());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1); // Параллелизм
        factory.getContainerProperties().setPollTimeout(3000); // Таймаут опроса
        factory.setCommonErrorHandler(errorHandler()); // Настраиваем обработку ошибок с ретраями
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        BackOff backOff = new FixedBackOff(
                properties.getConsumer().getRetryBackoffMs(),
                properties.getConsumer().getMaxRetries()
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> {
                    System.err.println("Failed to process record: " + record + ", exception: " + exception.getMessage());
                }, // Логирование ошибок
                backOff
        );
        return errorHandler;
    }
}

