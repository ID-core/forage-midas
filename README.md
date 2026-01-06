# Context

This work was done as part of a JPMorgan Chase Software Engineering job simulation, with hands-on implementation of backend components.
“The project was provided as a base repository, and I implemented the required components and integrations on top of it.”

# Midas Core - Transaction Processing System

A Spring Boot application that processes transactions through Kafka, integrates with an H2 database, and uses an incentives API for reward calculations.

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- The incentives API JAR file (included in `services/transaction-incentive-api.jar`)

## Project Structure

```
forage-midas/
├── src/
│   ├── main/java/com/jpmc/midascore/
│   │   ├── MidasCoreApplication.java         # Main application entry point
│   │   ├── component/
│   │   │   ├── DatabaseConduit.java          # Database operations
│   │   │   ├── KafkaConsumer.java            # Kafka message listener
│   │   │   └── IncentiveClient.java          # Incentives API client
│   │   ├── config/
│   │   │   └── RestConfig.java               # REST configuration
│   │   ├── entity/
│   │   │   ├── UserRecord.java               # User entity
│   │   │   └── TransactionRecord.java        # Transaction entity with incentives
│   │   ├── foundation/
│   │   │   ├── Transaction.java              # Transaction data model
│   │   │   └── Incentive.java                # Incentive data model
│   │   └── repository/
│   │       ├── UserRepository.java           # User data access
│   │       └── TransactionRepository.java    # Transaction data access
│   └── test/java/
│       └── com/jpmc/midascore/
│           ├── TaskThreeTests.java           # Test transactions without incentives
│           ├── TaskFourTests.java            # Test transactions with incentives
│           ├── KafkaProducer.java            # Test Kafka producer
│           ├── UserPopulator.java            # Test data loader
│           └── FileLoader.java               # File utility for tests
├── services/
│   └── transaction-incentive-api.jar         # Incentives API service
├── pom.xml                                    # Maven configuration
├── application.yml                            # Application configuration
└── README.md                                  # This file
```

## Building the Project

```bash
cd forage-midas
./mvnw clean install -DskipTests
```

This will compile the source code and run through the Maven build lifecycle without executing tests.

## Running the Application

### 1. Start the Incentives API Service

In a separate terminal, start the incentives API server (runs on port 8080):

```bash
cd services
java -jar transaction-incentive-api.jar
```

The API should start and display a message similar to:
```
Tomcat started on port(s): 8080 (http) with context path ''
```

### 2. Run the Main Application

The main Midas Core application starts automatically when needed by the tests, as it's a Spring Boot application.

### 3. Run Tests

#### Test Task Three: Process transactions without incentives

```bash
./mvnw test -Dtest=TaskThreeTests
```

This test:
- Loads user data from test data files
- Processes transactions from `test_data/mnbvcxz.vbnm`
- Validates transactions and updates user balances
- Displays the final balance of the "waldorf" user

Expected output includes:
```
WALDORF'S FINAL BALANCE: 627.86
WALDORF'S FINAL BALANCE (ROUNDED DOWN): 627
```

#### Test Task Four: Process transactions with incentives

First, ensure the incentives API is running (see step 1), then:

```bash
./mvnw test -Dtest=TaskFourTests
```

This test:
- Loads user data from test data files
- Processes transactions from `test_data/alskdjfh.fhdjsk`
- Calls the incentives API to calculate rewards for each transaction
- Updates user balances (adding incentives only to recipients)
- Displays the final balance of the "wilbur" user

Expected output includes:
```
WILBUR'S FINAL BALANCE: 3089.42
WILBUR'S FINAL BALANCE (ROUNDED DOWN): 3089
```

## Application Configuration

Configuration is managed in `application.yml`:

```yaml
general:
  kafka-topic: transactions

server:
  port: 33400

spring:
  datasource:
    url: jdbc:h2:mem:testdb
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: create-drop
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.type.mapping: transaction:com.jpmc.midascore.foundation.Transaction
    bootstrap-servers: localhost:9092
```

## Transaction Processing Flow

### Validation Rules

Each transaction is validated before processing:

1. **Sender Exists**: The sender ID must correspond to a valid user
2. **Recipient Exists**: The recipient ID must correspond to a valid user
3. **Sufficient Balance**: The sender must have a balance >= the transaction amount

If any validation fails, the transaction is discarded with no database changes.

### Processing Steps (with Incentives)

1. **Receive**: Transaction arrives via Kafka topic
2. **Validate**: Check sender, recipient, and balance
3. **Get Incentive**: Call the incentives API with the transaction details
4. **Update Balances**:
   - Deduct transaction amount from sender's balance
   - Add transaction amount + incentive amount to recipient's balance
5. **Record**: Save the transaction to the database with the incentive amount

### Database Schema

**UserRecord Table**:
- `id`: User identifier
- `name`: User's name
- `balance`: Current account balance

**TransactionRecord Table**:
- `id`: Transaction identifier
- `sender_id`: Foreign key to UserRecord (sender)
- `recipient_id`: Foreign key to UserRecord (recipient)
- `amount`: Transaction amount
- `incentive`: Incentive amount from the API

## Testing Data

Test data files are located in `src/test/resources/test_data/`:

- `lkjhgfdsa.hjkl`: User data (name, initial balance)
- `mnbvcxz.vbnm`: Transactions for Task Three
- `alskdjfh.fhdjsk`: Transactions for Task Four

## Troubleshooting

### Incentives API Not Responding

If you see connection errors to the incentives API:
1. Verify the API server is running on port 8080
2. Check that no other services are using port 8080
3. Ensure the jar file exists: `services/transaction-incentive-api.jar`

### Kafka Connection Issues

If Kafka connection fails:
1. The application uses an embedded Kafka for testing
2. For integration tests, ensure the previous test finished completely
3. Wait a few seconds between running consecutive tests

### Database Issues

The application uses an in-memory H2 database that is recreated for each test run. If you need to persist data:
1. Update `spring.jpa.hibernate.ddl-auto` in `application.yml` from `create-drop` to `update`
2. Change the datasource URL from `jdbc:h2:mem:testdb` to a file-based database

## Dependencies

Key dependencies managed by Maven:

- **Spring Boot 3.2.5**: Framework for building the application
- **Spring Data JPA**: Database ORM
- **Spring Kafka**: Kafka integration
- **H2 Database**: In-memory database for testing
- **JUnit 5**: Testing framework

## Running All Tests

To run all tests at once:

```bash
./mvnw test
```

Note: This requires the incentives API to be running for TaskFourTests to pass.

## Development Notes

- The application uses Spring's component scanning to automatically register services
- Transaction processing is asynchronous via Kafka listeners
- All database operations are performed within Kafka consumer threads
- The incentives API client includes error handling with a default 0 incentive if the API is unavailable
