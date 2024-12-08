package com.example.kafka.controller;

import com.example.kafka.producer.KafkaProducer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    private final KafkaProducer kafkaProducer;

    public KafkaController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @GetMapping("/fetch-and-publish")
    public String fetchAndPublish() {
        kafkaProducer.fetchAndPublish("product_topic");
        return "Data fetched from API and published to Kafka topic!";
    }
}
