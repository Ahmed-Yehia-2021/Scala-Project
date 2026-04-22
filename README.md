# Functional Retail Rule Engine

**Author:** Ahmed Yehia  
**Course:** Functional Programming with Scala - ITI 46 Project

## Project Overview
This project is a high-performance, strictly functional rule engine built for a large retail store. It processes massive batches of transaction data, evaluates each transaction against a series of qualifying discount rules, calculates the final price, and loads the results into a database. 

Initially designed to process 1,000 records, the engine has been successfully scaled to handle massive data pipelines processing 10 million orders per batch.

## Qualifying Rules
The engine evaluates each transaction against the following business rules:
* **Expiry Date:** For products with less than 30 days to expire from the transaction date: 29 days = 1% discount, 28 days = 2%, etc..
* **Category:** Cheese products receive a 10% discount, and Wine products receive a 5% discount.
* **Special Date:** Any product sold on March 23rd receives a massive 50% discount.
* **Quantity:** Buying 6-9 units yields a 5% discount, 10-14 units yields 7%, and more than 15 units yields 10%.

### Calculation Logic
* Transactions qualifying for multiple discounts will only receive the **top 2** discounts, which are then averaged.
* Transactions that do not qualify for any rules receive a 0% discount.

## Database Schema
The final evaluated transactions are loaded into a MySQL database. The system uses the following schema to store the processed results:

```sql
CREATE TABLE orders_discounts (
    id INT NOT NULL AUTO_INCREMENT,
    timestamp DATETIME NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    expiry_date DATE NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    discount DECIMAL(5,2) DEFAULT 0.00,
    PRIMARY KEY (id)
);
```

## Technical Considerations & Functional Constraints
This project adheres strictly to Functional Programming (FP) paradigms:
* **Immutability:** Only `val` assignments are used; `var` variables and mutable data structures are strictly prohibited.
* **Pure Functions:** All business logic functions are pure, meaning their output depends solely on the input, inputs are never mutated, and there are no side effects. Total functions are used to ensure a return value for every possible input.
* **No Loops:** Standard iterations (like `for` or `while` loops) are completely avoided in favor of functional combinators (`map`, `filter`, `foreach`, etc.).
* **Functional Error Handling:** Database integration and file I/O operations strictly utilize Scala's `Try` and `Using` constructs to encapsulate side effects and handle exceptions safely without breaking the functional flow.

## Scaling & Performance (10 Million Records)
To accommodate the scale-up to 10 million orders per batch, the following optimizations were implemented:
1.  **Lazy Evaluation Pipeline:** Replaced `.toList` with `Iterator` flows to process CSV files row-by-row, ensuring a tiny, stable memory footprint regardless of file size.
2.  **Super-Batching & Multi-Threading:** Data is chunked into "Super-Batches" and processed via a dedicated Scala `ExecutionContext` thread pool, allowing pure data transformations and database insertions to execute concurrently.
3.  **JDBC Batch Execution:** Isolated threads open their own database connections and utilize `executeBatch()` to drastically reduce database I/O network latency.
4.  **Resilient Data Parsing:** Implemented robust fallback parsing via `Try` to safely handle schema changes and varied date/timestamp formats between testing and production cohorts.

## Logging
The engine logs its lifecycle and processing events to `rules_engine.log`. It strictly follows the required format: `TIMESTAMP LOGLEVEL MESSAGE`.
