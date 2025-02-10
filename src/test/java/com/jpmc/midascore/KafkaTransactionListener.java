// filepath: /d:/openSource/forage-midas/src/main/java/com/jpmc/midascore/KafkaTransactionListener.java
package com.jpmc.midascore;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")

    public void listen(ConsumerRecord<String, String> record) {
        logger.info("Received transaction: " + record.value());
    }
}