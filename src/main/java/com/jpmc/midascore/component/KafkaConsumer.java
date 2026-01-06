package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private final IncentiveClient incentiveClient;

    public KafkaConsumer(UserRepository userRepository, TransactionRepository transactionRepository, IncentiveClient incentiveClient) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
        this.incentiveClient = incentiveClient;
    }

    @KafkaListener(topics = "transactions", groupId = "midas-group")
    public void consume(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate senderId and recipientId
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null) {
            logger.warn("Invalid senderId: {}", transaction.getSenderId());
            return;
        }

        if (recipient == null) {
            logger.warn("Invalid recipientId: {}", transaction.getRecipientId());
            return;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender: {} (balance: {}, amount: {})", 
                transaction.getSenderId(), sender.getBalance(), transaction.getAmount());
            return;
        }

        // Get incentive from API
        Incentive incentive = incentiveClient.getIncentive(transaction);
        float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

        // Process the transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);

        logger.info("Transaction processed successfully: {}", transaction);
    }
}
