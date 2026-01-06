package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveClient {
    static final Logger logger = LoggerFactory.getLogger(IncentiveClient.class);

    private final RestTemplate restTemplate;
    private final String incentiveUrl;

    public IncentiveClient(RestTemplate restTemplate, @Value("${incentive.api-url:http://localhost:8080/incentive}") String incentiveUrl) {
        this.restTemplate = restTemplate;
        this.incentiveUrl = incentiveUrl;
    }

    public Incentive getIncentive(Transaction transaction) {
        try {
            logger.info("Calling incentive API for transaction: {}", transaction);
            Incentive incentive = restTemplate.postForObject(incentiveUrl, transaction, Incentive.class);
            logger.info("Received incentive: {}", incentive);
            return incentive;
        } catch (Exception e) {
            logger.error("Error calling incentive API", e);
            return new Incentive(0);
        }
    }
}
