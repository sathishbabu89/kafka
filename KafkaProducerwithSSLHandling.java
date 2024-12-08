package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.*;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

@Service
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void fetchAndPublish(String topic) {
        RestTemplate restTemplate = getRestTemplateWithCertificate();
        String apiUrl = "https://dummyjson.com/products";
        String response = restTemplate.getForObject(apiUrl, String.class);
        kafkaTemplate.send(topic, response);
    }

    private RestTemplate getRestTemplateWithCertificate() {
        try {
            // Load the API's certificate
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            InputStream certInputStream = getClass().getResourceAsStream("/dummyjson-cert.crt"); // Add your cert to src/main/resources
            Certificate certificate = cf.generateCertificate(certInputStream);

            // Create a KeyStore with the certificate
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null); // Initialize KeyStore with no password
            keyStore.setCertificateEntry("dummyjson", certificate);

            // Create a TrustManagerFactory with the KeyStore
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            // Create an SSLContext with the TrustManagers
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);

            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());

            return new RestTemplate();
        } catch (Exception e) {
            throw new RuntimeException("Error setting up SSL context with custom certificate", e);
        }
    }
}
