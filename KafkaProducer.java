package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void fetchAndPublish(String topic) {
        RestTemplate restTemplate = new RestTemplate();
        String apiUrl = "https://dummyjson.com/products";
        String response = restTemplate.getForObject(apiUrl, String.class);
        kafkaTemplate.send(topic, response);
    }
}
