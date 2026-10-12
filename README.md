# Day 29 — End-to-End E-Commerce Data Pipeline

## Project Overview
An end-to-end e-commerce batch pipeline built using Scala and Apache Spark to clean, enrich, analyze, and store order data.

## Technologies
- Scala 2.13.16
- Apache Spark 3.5.6
- Spark Core and Pair RDD
- Spark SQL and DataFrames
- Broadcast joins and window functions
- Cache and persistence
- Partitioned Parquet storage
- SBT

## Problem Statement
Process raw order data with customer and product reference data, remove invalid orders, calculate revenue metrics, analyze customer order history, and store the processed results efficiently.

## Input Data
- `data/orders.csv`: Order details
- `data/customers.csv`: Customer details
- `data/products.csv`: Product details

## Architecture

```mermaid
flowchart TD
    A[Raw CSV Files] --> B[Read Data]
    B --> C[Clean Orders]
    C --> D[Pair RDD Aggregation]
    C --> E[Customer Join]
    E --> F[Broadcast Product Join]
    F --> G[Calculate Revenue]
    G --> H[Window and Aggregate Reports]
    G --> I[Partitioned Parquet Output]
