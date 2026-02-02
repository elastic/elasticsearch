/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.junit.BeforeClass;

/**
 * Base test class for standalone Parquet file tests (no Iceberg metadata).
 * <p>
 * This class uses templates in csv-spec files (like {@code {{employees}}}) that are
 * transformed to actual paths based on the storage backend (S3, HTTP, LOCAL).
 * <p>
 * The format is fixed to "parquet" for this test case.
 */
public abstract class ParquetSpecTestCase extends AbstractExternalSourceSpecTestCase {

    private static final Logger logger = LogManager.getLogger(ParquetSpecTestCase.class);

    @BeforeClass
    public static void verifyParquetFixturesLoaded() {
        logger.info("=== Verifying Parquet Fixtures ===");

        try {
            var logs = getRequestLogs();
            logger.info("Total fixture operations logged: {}", logs.size());

            boolean hasEmployeesParquet = logs.stream()
                .anyMatch(log -> log.getPath() != null && log.getPath().contains("standalone/employees.parquet"));

            if (hasEmployeesParquet) {
                logger.info("✓ standalone/employees.parquet found");
            } else {
                logger.warn("✗ standalone/employees.parquet NOT found - tests may fail");
            }

            long parquetFiles = logs.stream().filter(log -> log.getPath() != null && log.getPath().endsWith(".parquet")).count();

            logger.info("Fixture summary: {} Parquet files", parquetFiles);

        } catch (Exception e) {
            logger.error("Failed to verify fixtures", e);
        }

        logger.info("=== Parquet Setup Verification Complete ===");
    }

    protected ParquetSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend
    ) {
        // Format is always "parquet" for ParquetSpecTestCase
        super(fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend, "parquet");
    }
}
