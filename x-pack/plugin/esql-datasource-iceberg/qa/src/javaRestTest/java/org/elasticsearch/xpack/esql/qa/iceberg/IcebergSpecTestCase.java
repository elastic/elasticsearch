/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.iceberg;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.qa.rest.AbstractExternalSourceSpecTestCase;
import org.junit.BeforeClass;

/**
 * Base test class for Iceberg integration tests using S3HttpFixture.
 * Extends {@link AbstractExternalSourceSpecTestCase} with Iceberg-specific functionality.
 * <p>
 * Iceberg tests always use S3 storage backend since Iceberg requires metadata files.
 * The format is "iceberg" to indicate Iceberg table format (not standalone parquet).
 */
public abstract class IcebergSpecTestCase extends AbstractExternalSourceSpecTestCase {

    private static final Logger logger = LogManager.getLogger(IcebergSpecTestCase.class);

    /**
     * Verify that Iceberg fixtures were loaded successfully.
     */
    @BeforeClass
    public static void verifyIcebergFixturesLoaded() {
        logger.info("=== Verifying Iceberg Fixtures ===");

        try {
            var logs = getRequestLogs();
            logger.info("Total fixture operations logged: {}", logs.size());

            boolean hasEmployeesMetadata = logs.stream()
                .anyMatch(log -> log.getPath() != null && log.getPath().contains("employees/metadata"));

            boolean hasEmployeesParquet = logs.stream()
                .anyMatch(log -> log.getPath() != null && log.getPath().contains("standalone/employees.parquet"));

            if (hasEmployeesMetadata) {
                logger.info("✓ employees Iceberg table metadata found - using Iceberg format");
            } else if (hasEmployeesParquet) {
                logger.info("✓ standalone/employees.parquet found - using legacy Parquet format");
            } else {
                logger.warn("✗ employees fixture NOT found - tests may fail");
            }

            long parquetFiles = logs.stream().filter(log -> log.getPath() != null && log.getPath().endsWith(".parquet")).count();
            long metadataFiles = logs.stream().filter(log -> log.getPath() != null && log.getPath().contains("metadata")).count();

            logger.info("Fixture summary: {} Parquet files, {} metadata files", parquetFiles, metadataFiles);

        } catch (Exception e) {
            logger.error("Failed to verify fixtures", e);
        }

        logger.info("=== Iceberg Setup Verification Complete ===");
    }

    protected IcebergSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        // Iceberg tests use S3 storage backend and "iceberg" format (no template transformation needed)
        super(fileName, groupName, testName, lineNumber, testCase, instructions, StorageBackend.S3, "iceberg");
    }

    /**
     * Verifies that Iceberg metadata files were accessed during test execution.
     */
    protected void verifyIcebergMetadataUsed() {
        var logs = getRequestLogs();

        boolean accessedMetadataJson = logs.stream().anyMatch(log -> log.getPath() != null && log.getPath().contains("metadata.json"));

        boolean accessedManifestList = logs.stream().anyMatch(log -> log.getPath() != null && log.getPath().contains("/metadata/snap-"));

        boolean accessedManifest = logs.stream().anyMatch(log -> log.getPath() != null && log.getPath().matches(".*metadata/.*\\.avro"));

        logger.info("Iceberg metadata usage verification:");
        logger.info("  - Metadata JSON accessed: {}", accessedMetadataJson);
        logger.info("  - Manifest list accessed: {}", accessedManifestList);
        logger.info("  - Manifest file accessed: {}", accessedManifest);

        if (accessedMetadataJson || accessedManifestList || accessedManifest) {
            logger.info("✓ Confirmed using Iceberg table format");
        } else {
            logger.warn("✗ No Iceberg metadata files accessed - may be using standalone Parquet format");
        }
    }

    /**
     * Returns true if Iceberg metadata was used in the current test.
     */
    protected boolean wasIcebergMetadataUsed() {
        var logs = getRequestLogs();
        return logs.stream()
            .anyMatch(
                log -> log.getPath() != null
                    && (log.getPath().contains("metadata.json")
                        || log.getPath().contains("/metadata/snap-")
                        || log.getPath().matches(".*metadata/.*\\.avro"))
            );
    }

    /**
     * Creates an S3FileIO configured to use the S3HttpFixture.
     */
    protected static S3FileIO createS3FileIO() {
        return S3FixtureUtils.createS3FileIO(s3Fixture.getAddress());
    }
}
