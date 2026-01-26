# Iceberg Integration Tests

This module contains integration tests for ESQL Iceberg functionality using S3HttpFixture.

## Overview

These tests run ESQL queries against real Iceberg tables backed by S3 (mocked via S3HttpFixture) and Parquet files. They provide end-to-end testing of the full stack:

- REST API → Query Planning → Iceberg Source → S3 → Parquet

## Structure

- `src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/iceberg/` - Integration test classes
  - `IcebergSpecIT.java` - Main spec test class extending EsqlSpecTestCase
  - `Clusters.java` - Test cluster configuration

## Running Tests

```bash
./gradlew :x-pack:plugin:esql:qa:server:iceberg:javaRestTest
```

## Dependencies

- S3HttpFixture for mocking S3 operations
- Iceberg libraries (core, aws, parquet)
- Hadoop client for catalog support
- AWS SDK for S3 access
- Repository-S3 module for cluster

## Test Data

Test fixtures are loaded from `src/test/resources/iceberg-fixtures/` in the main esql module.
