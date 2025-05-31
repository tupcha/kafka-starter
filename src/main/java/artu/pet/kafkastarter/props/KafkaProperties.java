package artu.pet.kafkastarter.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "custom.kafka")
@Getter
@Setter
public class KafkaProperties {

    private boolean enabled = true; // Включение/отключение автоконфигурации
    private String bootstrapServers;
    private CustomProducerProp producer;
    private CustomConsumerProp consumer;
    private String avroSchemaPath;

    @Getter
    @Setter
    public static class CustomProducerProp {
        private String topic = "default-topic";
        private boolean idempotenceEnabled = true; // Идемпотентность для надежности
        private int acks = 1; // Подтверждения: 0, 1, all
        private int retries = 3; // Количество попыток при сбоях
        private int retryBackoffMs = 1000; // Задержка между ретраями
        private int deliveryTimeoutMs = 120000; // Таймаут доставки
        private int requestTimeoutMs = 30000; // Таймаут запроса
        private int maxInFlightRequests = 5; // Макс. количество неподтвержденных запросов
        private Map<String, Object> config = new HashMap<>(); // Дополнительные настройки
    }

    @Getter
    @Setter
    public static class CustomConsumerProp {
        private String topic = "default-topic";
        private String groupId = "default-group";
        private int maxPollRecords = 500; // Макс. Записей за один poll
        private int maxPollIntervalMs = 300000; // Макс. Интервал между poll
        private int sessionTimeoutMs = 10000; // Таймаут сессии
        private int heartbeatIntervalMs = 3000; // Интервал heartbeat
        private int fetchMaxBytes = 52428800; // Макс. размер выборки (50MB)
        private boolean autoCommitEnabled = true; // Автокоммит оффсетов
        private int autoCommitIntervalMs = 5000; // Интервал автокоммита
        private String autoOffsetReset = "earliest"; // Сброс оффсета
        private int retryBackoffMs = 1000; // Задержка между ретраями
        private int maxRetries = 3; // Количество ретраев
        private Map<String, Object> config = new HashMap<>(); // Дополнительные настройки
    }
}
