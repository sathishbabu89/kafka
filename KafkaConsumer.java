package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @KafkaListener(topics = "product_topic", groupId = "group_id")
    public void consume(String message) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode products = objectMapper.readTree(message).get("products");
            for (JsonNode product : products) {
                int id = product.get("id").asInt();
                String title = product.get("title").asText();
                String description = product.get("description").asText();
                double price = product.get("price").asDouble();

                jdbcTemplate.update(
                    "INSERT INTO products (id, title, description, price) VALUES (?, ?, ?, ?)",
                    id, title, description, price
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
