package artu.pet.kafkastarter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
public class KafkaStarterApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaStarterApplication.class, args);
    }

}
