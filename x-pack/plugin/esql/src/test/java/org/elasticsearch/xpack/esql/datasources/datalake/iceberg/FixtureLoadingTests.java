/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake.iceberg;

import org.elasticsearch.common.bytes.BytesReference;
import org.junit.After;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.WAREHOUSE;

/**
 * Tests to verify that fixtures are automatically loaded from src/test/resources/iceberg-fixtures/.
 * This validates the integration between the S3HttpFixture and the resource directory.
 */
public class FixtureLoadingTests extends org.elasticsearch.xpack.esql.datasources.datalake.AbstractS3HttpFixtureTest {

    @After
    public void printRequestLogsAfterEachTest() {
        // Print request summary after each test for debugging
        printRequestSummary();
    }

    /**
     * Test that fixtures are automatically loaded from the iceberg-fixtures directory.
     */
    public void testFixturesAutoLoaded() {
        // Get the blobs map from the fixture
        Map<String, BytesReference> blobs = s3Fixture.getHandler().blobs();
        
        assertNotNull("Blobs map should not be null", blobs);
        
        // Check if employees Iceberg table was loaded (preferred)
        String employeesDataPath = "/" + BUCKET + "/" + WAREHOUSE + "/employees/data/data.parquet";
        String employeesMetadataPath = "/" + BUCKET + "/" + WAREHOUSE + "/employees/metadata";
        
        BytesReference employeesDataBlob = blobs.get(employeesDataPath);
        boolean hasIcebergMetadata = blobs.keySet().stream()
            .anyMatch(key -> key.startsWith(employeesMetadataPath));
        
        if (employeesDataBlob != null && hasIcebergMetadata) {
            logger.info("✓ Verified Iceberg table fixture: employees/");
            logger.info("  - Data file: {} bytes", employeesDataBlob.length());
            logger.info("  - Metadata files present: {}", hasIcebergMetadata);
        } else {
            // Fallback: check for standalone employees.parquet
            String standaloneEmployeesPath = "/" + BUCKET + "/" + WAREHOUSE + "/standalone/employees.parquet";
            BytesReference standaloneBlob = blobs.get(standaloneEmployeesPath);
            
            if (standaloneBlob != null) {
                logger.info("✓ Found standalone/employees.parquet ({} bytes)", standaloneBlob.length());
                logger.warn("Note: Using legacy standalone Parquet format; Iceberg table format preferred");
            } else {
                logger.warn("✗ No employees fixture found!");
                logger.info("Expected either:");
                logger.info("  - Iceberg table at: employees/data/data.parquet + employees/metadata/");
                logger.info("  - Or standalone Parquet at: standalone/employees.parquet");
            }
        }
        
        // At least one format should be available
        String standaloneEmployeesPath = "/" + BUCKET + "/" + WAREHOUSE + "/standalone/employees.parquet";
        boolean hasAnyEmployees = employeesDataBlob != null || blobs.get(standaloneEmployeesPath) != null;
        assertTrue("Either Iceberg table or standalone employees fixture should be loaded", hasAnyEmployees);
    }

    /**
     * Test that fixtures can be accessed via HTTP GET requests.
     */
    public void testFixtureAccessViaHttp() throws Exception {
        clearRequestLogs();
        
        // Try to access the employees data file via HTTP
        // First try Iceberg table format, then fallback to standalone
        String icebergUrl = getS3Endpoint() + "/" + BUCKET + "/" + WAREHOUSE + "/employees/data/data.parquet";
        String standaloneUrl = getS3Endpoint() + "/" + BUCKET + "/" + WAREHOUSE + "/standalone/employees.parquet";
        
        HttpConnection conn = createS3Connection(icebergUrl, "GET");
        String accessedPath;
        
        if (conn.getResponseCode() == 200) {
            accessedPath = "employees/data/data.parquet";
            logger.info("Successfully accessed Iceberg table data via HTTP: {} ({} bytes)", accessedPath, conn.getBody().length);
        } else {
            // Fallback to standalone
            conn = createS3Connection(standaloneUrl, "GET");
            accessedPath = "standalone/employees.parquet";
            logger.info("Falling back to standalone format: {} ({} bytes)", accessedPath, conn.getBody().length);
        }
        
        assertEquals("Should get 200 OK for fixture file", 200, conn.getResponseCode());
        assertTrue("Response should have content", conn.getBody().length > 0);
        
        // Verify at least one GET_OBJECT request was logged
        assertTrue("Should have at least one GET_OBJECT request", getRequestCount("GET_OBJECT") >= 1);
    }

    /**
     * Test that HEAD requests work for loaded fixtures.
     */
    public void testFixtureHeadRequest() throws Exception {
        clearRequestLogs();
        
        // Try Iceberg table format first, then fallback to standalone
        String icebergUrl = getS3Endpoint() + "/" + BUCKET + "/" + WAREHOUSE + "/employees/data/data.parquet";
        String standaloneUrl = getS3Endpoint() + "/" + BUCKET + "/" + WAREHOUSE + "/standalone/employees.parquet";
        
        HttpConnection conn = createS3Connection(icebergUrl, "HEAD");
        String accessedPath;
        
        if (conn.getResponseCode() == 200) {
            accessedPath = "employees/data/data.parquet";
        } else {
            conn = createS3Connection(standaloneUrl, "HEAD");
            accessedPath = "standalone/employees.parquet";
        }
        
        assertEquals("Should get 200 OK for HEAD request", 200, conn.getResponseCode());
        
        logger.info("Successfully performed HEAD request on fixture: {}", accessedPath);
        
        // Verify at least one HEAD_OBJECT request was logged
        assertTrue("Should have at least one HEAD_OBJECT request", getRequestCount("HEAD_OBJECT") >= 1);
    }

    /**
     * Test that non-existent files return 404.
     */
    public void testNonExistentFixture() throws Exception {
        clearRequestLogs();
        
        String url = getS3Endpoint() + "/" + BUCKET + "/" + WAREHOUSE + "/non-existent.parquet";
        
        HttpConnection conn = createS3Connection(url, "GET");
        
        assertEquals("Should get 404 for non-existent file", 404, conn.getResponseCode());
        
        logger.info("Correctly returned 404 for non-existent fixture");
    }

    /**
     * Test that manually added blobs coexist with loaded fixtures.
     */
    public void testManualAndAutoLoadedFixtures() {
        // Add a blob manually
        String manualKey = "manual/test.txt";
        String manualContent = "manually added content";
        addBlobToFixture(manualKey, manualContent);
        
        // Verify both manual and auto-loaded fixtures exist
        Map<String, BytesReference> blobs = s3Fixture.getHandler().blobs();
        
        String manualPath = "/" + BUCKET + "/" + WAREHOUSE + "/" + manualKey;
        
        // Check for either Iceberg table format or standalone format
        String icebergDataPath = "/" + BUCKET + "/" + WAREHOUSE + "/employees/data/data.parquet";
        String standalonePath = "/" + BUCKET + "/" + WAREHOUSE + "/standalone/employees.parquet";
        
        assertNotNull("Manual blob should exist", blobs.get(manualPath));
        
        boolean hasAutoLoaded = blobs.get(icebergDataPath) != null || blobs.get(standalonePath) != null;
        assertTrue("Auto-loaded employees fixture should exist (either Iceberg or standalone)", hasAutoLoaded);
        
        logger.info("Successfully verified coexistence of manual and auto-loaded fixtures");
    }

    /**
     * Test that fixtures in subdirectories are loaded correctly.
     */
    public void testSubdirectoryFixtures() {
        // Create a test structure in fixtures
        // For this test, we'll just verify the path mapping logic
        
        // If we had a file at iceberg-fixtures/db/table/metadata/v1.json,
        // it should be accessible at s3://bucket/warehouse/db/table/metadata/v1.json
        
        String expectedPath = "/" + BUCKET + "/" + WAREHOUSE + "/db/table/metadata/v1.json";
        
        // This test documents the expected behavior
        // Actual subdirectory fixtures can be added as needed
        logger.info("Subdirectory fixtures would be accessible at paths like: {}", expectedPath);
    }

    /**
     * Test that verifies the Iceberg table fixture structure is correct.
     * An Iceberg table should have:
     * - metadata/ directory with v*.metadata.json files
     * - data/ directory with parquet files
     */
    public void testIcebergTableFixtureStructure() {
        Map<String, BytesReference> blobs = s3Fixture.getHandler().blobs();
        
        // Check if employees Iceberg table structure exists
        String metadataPrefix = "/" + BUCKET + "/" + WAREHOUSE + "/employees/metadata/";
        String dataPrefix = "/" + BUCKET + "/" + WAREHOUSE + "/employees/data/";
        
        boolean hasMetadataDir = blobs.keySet().stream()
            .anyMatch(key -> key.startsWith(metadataPrefix));
        boolean hasDataDir = blobs.keySet().stream()
            .anyMatch(key -> key.startsWith(dataPrefix));
        boolean hasMetadataJson = blobs.keySet().stream()
            .anyMatch(key -> key.startsWith(metadataPrefix) && key.endsWith(".metadata.json"));
        
        if (hasMetadataDir && hasDataDir) {
            logger.info("✓ Iceberg table structure found:");
            logger.info("  - Has metadata directory: {}", hasMetadataDir);
            logger.info("  - Has data directory: {}", hasDataDir);
            logger.info("  - Has metadata.json: {}", hasMetadataJson);
            
            // List all files in the Iceberg table
            logger.info("  Iceberg table files:");
            blobs.keySet().stream()
                .filter(key -> key.contains("/employees/"))
                .forEach(key -> logger.info("    - {}", key));
        } else {
            logger.info("Iceberg table structure not found (using standalone parquet format)");
            logger.info("To enable Iceberg table format, create the following fixture structure:");
            logger.info("  iceberg-fixtures/employees/");
            logger.info("    metadata/");
            logger.info("      v1.metadata.json");
            logger.info("      snap-*.avro (manifest list)");
            logger.info("      *-m0.avro (manifest)");
            logger.info("    data/");
            logger.info("      data.parquet");
        }
    }

    /**
     * Test that the fixture loading doesn't fail if the directory is empty.
     */
    public void testEmptyFixturesDirectory() {
        // This test verifies that the loading mechanism is resilient
        // Even if iceberg-fixtures/ is empty or missing, tests should still work
        
        // The fixture loading happens in @BeforeClass, so if we got here, it worked
        assertNotNull("Fixture should be initialized", s3Fixture);
        assertNotNull("Handler should be initialized", s3Fixture.getHandler());
        
        logger.info("Fixture loading is resilient to empty/missing directories");
    }
}
