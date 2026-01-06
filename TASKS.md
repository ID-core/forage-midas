# Midas Core - Project Tasks and Problems

This document outlines the problems that needed to be solved and the implementations required for the Midas Core transaction processing system.

## Task 1: Set Up H2 Database Integration

### Problem
The application needed to persist user and transaction data in a relational database rather than keeping it in memory.

### Solution
- Integrated Spring Data JPA with H2 database
- Created `UserRecord` entity to represent users in the database with:
  - User ID (auto-generated primary key)
  - User name
  - Current balance
- Configured H2 as an in-memory database for testing with automatic schema creation/deletion
- Created `UserRepository` interface extending `CrudRepository` for database access

### Key Files
- `src/main/java/com/jpmc/midascore/entity/UserRecord.java`
- `src/main/java/com/jpmc/midascore/repository/UserRepository.java`
- `application.yml` - Database configuration

---

## Task 2: Implement Transaction Validation and Recording

### Problem
Transactions received via Kafka needed to be:
1. Validated based on sender/recipient existence and balance availability
2. Applied to the database only if valid
3. Discarded without any database changes if invalid
4. Recorded with a many-to-one relationship to both sender and recipient users

### Solution
- Created `TransactionRecord` entity with:
  - Many-to-one relationship to UserRecord for sender
  - Many-to-one relationship to UserRecord for recipient
  - Transaction amount field
  - Incentive amount field (for later use)
- Implemented `KafkaConsumer` component to:
  - Listen to Kafka messages containing transactions
  - Validate transactions:
    - Check if sender exists in database
    - Check if recipient exists in database
    - Verify sender has sufficient balance
  - Update user balances only if all validations pass
  - Record valid transactions to the database
  - Log and discard invalid transactions
- Created `TransactionRepository` for database access

### Validation Rules
A transaction is considered **valid** if ALL of the following are true:
- The `senderId` corresponds to an existing user
- The `recipientId` corresponds to an existing user
- The sender's balance >= transaction amount

### Balance Updates
When a transaction is valid:
- Sender's balance is decreased by the transaction amount
- Recipient's balance is increased by the transaction amount
- Both updated balances are persisted to the database

### Key Files
- `src/main/java/com/jpmc/midascore/entity/TransactionRecord.java`
- `src/main/java/com/jpmc/midascore/repository/TransactionRepository.java`
- `src/main/java/com/jpmc/midascore/component/KafkaConsumer.java`

### Test Results
**Task Three Test** (`TaskThreeTests.java`)
- Processes 22 transactions from test data
- Validates and applies only valid transactions
- Final result: Waldorf's balance = **627** (rounded down)

---

## Task 3: Integrate External Incentives API

### Problem
After validating a transaction, the system needed to:
1. Call an external REST API to determine incentive amounts
2. Add incentive amounts to recipient balances (but NOT deduct from sender)
3. Record incentive amounts alongside transaction data

### Solution
- Created `Incentive` data model class to represent API responses
- Created `IncentiveClient` component to:
  - Make HTTP POST requests to the incentives API
  - Send the complete `Transaction` object as JSON
  - Receive `Incentive` objects with an `amount` field
  - Handle API failures gracefully by returning 0 incentive
  - Log API calls and responses
- Created `RestConfig` configuration class to provide a `RestTemplate` bean
- Updated `KafkaConsumer` to:
  - Call `IncentiveClient` after validation but before balance updates
  - Add incentive amount to recipient's balance
  - Save incentive amount with the transaction record
- Updated `TransactionRecord` to include an `incentive` field

### API Specification
- **URL**: `http://localhost:8080/incentive`
- **Method**: POST
- **Request**: JSON serialized `Transaction` object
- **Response**: JSON serialized `Incentive` object with `amount` field (>= 0)

### Balance Update Flow (with Incentives)
1. Validate transaction (sender, recipient, balance)
2. Call incentives API
3. Update balances:
   - Sender: balance -= transaction amount
   - Recipient: balance += (transaction amount + incentive amount)
4. Record transaction with incentive amount

### Key Files
- `src/main/java/com/jpmc/midascore/foundation/Incentive.java`
- `src/main/java/com/jpmc/midascore/component/IncentiveClient.java`
- `src/main/java/com/jpmc/midascore/config/RestConfig.java`
- `services/transaction-incentive-api.jar` - External API service

### Test Results
**Task Four Test** (`TaskFourTests.java`)
- Processes transactions and calls incentives API for each valid transaction
- Incentive API returns varying amounts (0 to 5 units)
- Final result: Wilbur's balance = **3089** (rounded down)

---

## Technical Challenges Solved

### 1. Kafka Serialization
**Challenge**: Spring Kafka was trying to serialize Transaction objects using StringSerializer
**Solution**: Configured Kafka to use JsonSerializer for values and JsonDeserializer for deserialization with proper type mapping

### 2. Transaction-to-Entity Mapping
**Challenge**: The existing `Transaction` class (POJO) couldn't be used directly with JPA
**Solution**: Created a separate `TransactionRecord` entity with the same fields plus relationships to UserRecord entities

### 3. Relationship Management
**Challenge**: Properly managing many-to-one relationships in the database
**Solution**: Used JPA's `@ManyToOne` and `@JoinColumn` annotations to create foreign key constraints to UserRecord

### 4. External API Integration
**Challenge**: Integrating with an external REST API while maintaining transaction processing asynchronously
**Solution**: Created an `IncentiveClient` component that wraps RestTemplate with error handling

### 5. Data Flow Architecture
**Challenge**: Coordinating between Kafka consumers, database operations, and API calls
**Solution**:
- `KafkaConsumer` handles orchestration
- `IncentiveClient` handles API communication
- `DatabaseConduit` handles data persistence
- Clear separation of concerns using dependency injection

---

## Database Schema

### UserRecord Table
| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT |
| name | VARCHAR | NOT NULL |
| balance | FLOAT | NOT NULL |

### TransactionRecord Table
| Column | Type | Constraints |
|--------|------|-------------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT |
| sender_id | BIGINT | FOREIGN KEY -> UserRecord(id), NOT NULL |
| recipient_id | BIGINT | FOREIGN KEY -> UserRecord(id), NOT NULL |
| amount | FLOAT | NOT NULL |
| incentive | FLOAT | NOT NULL |

---

## Error Handling and Edge Cases

### Handled Scenarios
1. **Invalid Sender**: Transaction discarded, no database changes
2. **Invalid Recipient**: Transaction discarded, no database changes
3. **Insufficient Balance**: Transaction discarded, no database changes
4. **API Unavailable**: Gracefully defaults to 0 incentive, transaction still processed
5. **API Timeout**: Logs error and uses 0 incentive, allows transaction to proceed
6. **Malformed Messages**: Kafka deserialization errors caught and logged

### Logging
All components use SLF4J for comprehensive logging:
- Transaction received notifications
- Validation failures with reasons
- API calls and responses
- Processing success confirmations
- Errors and exceptions

---

## Configuration Points

### Kafka Configuration
```yaml
spring.kafka.producer.value-serializer: JsonSerializer
spring.kafka.consumer.value-deserializer: JsonDeserializer
general.kafka-topic: transactions
```

### Database Configuration
```yaml
spring.datasource.url: jdbc:h2:mem:testdb
spring.jpa.hibernate.ddl-auto: create-drop
```

### API Configuration
```yaml
incentive.api-url: http://localhost:8080/incentive (default)
```

---

## Summary of Implementations

| Component | Purpose | Status |
|-----------|---------|--------|
| UserRecord Entity | Represent users in database |  Complete |
| TransactionRecord Entity | Represent transactions with incentives |  Complete |
| UserRepository | Database access for users |  Complete |
| TransactionRepository | Database access for transactions |  Complete |
| KafkaConsumer | Process incoming transactions |  Complete |
| IncentiveClient | Call incentives API |  Complete |
| RestConfig | REST client configuration |  Complete |
| TaskThreeTests | Validate without incentives |  Complete |
| TaskFourTests | Validate with incentives |  Complete |

All tasks have been successfully completed with full integration testing.
