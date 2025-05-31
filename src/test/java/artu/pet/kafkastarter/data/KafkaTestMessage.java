package artu.pet.kafkastarter.data;

import lombok.Builder;

import java.io.Serializable;

@Builder(toBuilder = true)
public record KafkaTestMessage(
        String id,
        String content,
        long timestamp
) implements Serializable {
}
