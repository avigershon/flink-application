# Flink Application

This project provides a simple Java Flink 1.21 application. It reads from a table defined at runtime, converts it to a DataStream to replace the value of a specified field, and writes the modified stream to a Kafka topic.

## Requirements

- Java 8 or later
- Maven
- Kafka running and reachable

## Build

Use Maven to build the project:

```bash
mvn package
```

## Usage

Run the application with the required arguments:

```bash
java -cp target/flink-application-0.1-SNAPSHOT.jar com.example.FlinkTableStreamer \
  --source_ddl "<CREATE TABLE ... AS SELECT ...>" \
  --sink_ddl   "<CREATE TABLE ... AS SELECT ...>" \
  --field my_field \
  --value new_value
```


The provided DDL strings may include a trailing `AS SELECT` clause. Only the `CREATE TABLE` portion is used to register the source and sink tables. The application converts the source table to a DataStream, replaces the selected field's value using the DataStream API, and writes the modified stream to the sink table.
