# Flink Application

This project provides a simple Java Flink 2.0 application. It executes a list of DDL statements at startup, registers a few example user-defined functions, and then reads from the `users` table to produce processed rows that are written to the `casino_activities` table.

## Requirements

- Java 8 or later
- Maven
- Kafka running and reachable

## Build

Use Maven to build the project:

```bash
mvn package
```

The resulting JAR excludes the core Flink libraries, so run it on a
Flink 2.0 cluster that already supplies these dependencies.

## Usage

Run the application with the required arguments:

```bash
java -cp target/casino-activities-bundled-1.0.jar bi.flink.MainApp \
  --dbt.ddl "<DDL statement 1>\n<DDL statement 2>"
```


The DDL string may contain multiple statements separated by newlines. Each statement is executed in order to create the required catalog objects before the job runs. After the setup phase the application processes the `users` table using the provided user-defined functions and inserts the results into `casino_activities`.
