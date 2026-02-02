/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.format.parquet;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;
import org.elasticsearch.xpack.esql.datasources.datalake.AbstractS3HttpFixtureTest;
import org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.http.HttpConfiguration;
import org.elasticsearch.xpack.esql.datasources.http.HttpStorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration test for Parquet over S3 (via HTTP).
 *
 * Tests the complete flow: HTTP storage provider (pointing to S3 fixture) -> Parquet format reader -> ESQL Pages
 *
 * This test validates:
 * - HttpStorageProvider can access objects from S3HttpFixture
 * - ParquetFormatReader can read Parquet files from HTTP/S3
 * - Range reads work correctly for columnar access
 * - Column projection works with HTTP storage
 * - Schema extraction works over HTTP
 *
 * Note: We use HttpStorageProvider instead of S3StorageProvider to avoid AWS SDK entitlement issues in tests.
 * The S3HttpFixture provides an S3-compatible HTTP interface that works with standard HTTP clients.
 *
 * Note: HttpClient creates background selector threads that cannot be easily shut down.
 * These are daemon threads and don't prevent JVM shutdown, so we suppress thread leak warnings.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class S3ParquetIntegrationTests extends AbstractS3HttpFixtureTest {

    private BlockFactory blockFactory;
    private HttpStorageProvider storageProvider;
    private ExecutorService executor;

    @Before
    public void setUpTest() throws Exception {
        blockFactory = BlockFactory.getInstance(new NoopCircuitBreaker("test-noop"), BigArrays.NON_RECYCLING_INSTANCE);

        executor = Executors.newFixedThreadPool(2);

        // Create HTTP storage provider configured for the S3 fixture
        // The S3HttpFixture provides an S3-compatible HTTP interface
        // Add authorization header for S3HttpFixture
        String authHeader = "AWS4-HMAC-SHA256 Credential="
            + S3FixtureUtils.ACCESS_KEY
            + "/20260126/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=dummy";

        HttpConfiguration httpConfig = HttpConfiguration.builder()
            .connectTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(30))
            .customHeaders(java.util.Map.of("Authorization", authHeader))
            .build();

        storageProvider = new HttpStorageProvider(httpConfig, executor);

        // Clear request logs before each test
        clearRequestLogs();
    }

    @Override
    public void tearDown() throws Exception {
        if (storageProvider != null) {
            storageProvider.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        super.tearDown();
    }

    public void testReadParquetFromS3ViaHttp() throws Exception {
        // Load the test Parquet file from resources
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add to S3 fixture
        String s3Key = "test-data/employees.parquet";
        addBlobToFixture(s3Key, parquetData);

        // Create HTTP storage path (S3HttpFixture is accessible via HTTP)
        String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
        StoragePath path = StoragePath.of(httpUrl);
        StorageObject storageObject = storageProvider.newObject(path);

        // Verify object exists and has correct length
        assertTrue("Object should exist", storageObject.exists());
        assertEquals("Object length should match", parquetData.length, storageObject.length());

        // Create Parquet format reader
        ParquetFormatReader formatReader = new ParquetFormatReader(blockFactory);

        // Test schema reading
        List<Attribute> schema = formatReader.getSchema(storageObject);
        assertNotNull("Schema should not be null", schema);
        assertTrue("Schema should have columns", schema.size() > 0);

        logger.info("Successfully read schema with {} columns from S3 Parquet file", schema.size());

        // Test data reading
        clearRequestLogs(); // Clear logs to isolate data read requests

        try (CloseableIterator<Page> iterator = formatReader.read(storageObject, null, 100)) {
            assertTrue("Should have at least one page", iterator.hasNext());

            Page page = iterator.next();
            assertNotNull("Page should not be null", page);
            assertTrue("Page should have rows", page.getPositionCount() > 0);
            assertTrue("Page should have columns", page.getBlockCount() > 0);

            logger.info(
                "Successfully read {} rows with {} columns from S3 Parquet file via HTTP",
                page.getPositionCount(),
                page.getBlockCount()
            );
        }

        // Print request summary for analysis
        printRequestSummary();
    }

    public void testParquetRangeReadsFromS3() throws Exception {
        // Load the test Parquet file
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add to S3 fixture
        String s3Key = "test-data/employees-range.parquet";
        addBlobToFixture(s3Key, parquetData);

        // Create HTTP storage path
        String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
        StoragePath path = StoragePath.of(httpUrl);
        StorageObject storageObject = storageProvider.newObject(path);

        // Create Parquet format reader
        ParquetFormatReader formatReader = new ParquetFormatReader(blockFactory);

        clearRequestLogs();

        // Read data - Parquet should use range reads for columnar access
        try (CloseableIterator<Page> iterator = formatReader.read(storageObject, null, 100)) {
            while (iterator.hasNext()) {
                Page page = iterator.next();
                assertNotNull("Page should not be null", page);
            }
        }

        // Verify that GET requests were made
        List<S3FixtureUtils.S3RequestLog> getRequests = getRequestsByType("GET_OBJECT");
        assertFalse("Should have made GET_OBJECT requests", getRequests.isEmpty());

        // Check if any requests had Range headers (Parquet may use range reads for column chunks)
        boolean hasRangeRequests = getRequests.stream().anyMatch(log -> log.getHeaders().containsKey("Range"));

        logger.info(
            "Parquet read made {} GET_OBJECT requests, {} with Range headers",
            getRequests.size(),
            hasRangeRequests ? "some" : "none"
        );

        printRequestSummary();
    }

    public void testParquetColumnProjectionFromS3() throws Exception {
        // Load the test Parquet file
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add to S3 fixture
        String s3Key = "test-data/employees-projection.parquet";
        addBlobToFixture(s3Key, parquetData);

        // Create HTTP storage path
        String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
        StoragePath path = StoragePath.of(httpUrl);
        StorageObject storageObject = storageProvider.newObject(path);

        // Create Parquet format reader
        ParquetFormatReader formatReader = new ParquetFormatReader(blockFactory);

        // Get full schema first
        List<Attribute> fullSchema = formatReader.getSchema(storageObject);
        assertTrue("Should have multiple columns", fullSchema.size() >= 2);

        // Get first two column names for projection
        String col1 = fullSchema.get(0).name();
        String col2 = fullSchema.get(1).name();

        clearRequestLogs();

        // Read with column projection
        try (CloseableIterator<Page> iterator = formatReader.read(storageObject, List.of(col1, col2), 100)) {
            assertTrue("Should have at least one page", iterator.hasNext());

            Page page = iterator.next();
            assertNotNull("Page should not be null", page);
            assertEquals("Should only have projected columns", 2, page.getBlockCount());

            logger.info("Successfully read projected columns {} and {} from S3 Parquet file", col1, col2);
        }

        printRequestSummary();
    }

    public void testParquetSchemaExtractionFromS3() throws Exception {
        // Load the test Parquet file
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add to S3 fixture
        String s3Key = "test-data/employees-schema.parquet";
        addBlobToFixture(s3Key, parquetData);

        // Create HTTP storage path
        String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
        StoragePath path = StoragePath.of(httpUrl);
        StorageObject storageObject = storageProvider.newObject(path);

        // Create Parquet format reader
        ParquetFormatReader formatReader = new ParquetFormatReader(blockFactory);

        clearRequestLogs();

        // Extract schema
        List<Attribute> schema = formatReader.getSchema(storageObject);

        assertNotNull("Schema should not be null", schema);
        assertTrue("Schema should have columns", schema.size() > 0);

        // Verify each column has a name and type
        for (Attribute attr : schema) {
            assertNotNull("Column name should not be null", attr.name());
            assertNotNull("Column type should not be null", attr.dataType());
            logger.info("Column: {} (type: {})", attr.name(), attr.dataType());
        }

        // Schema extraction should be efficient - minimal HTTP requests
        int totalRequests = getRequestLogs().size();
        logger.info("Schema extraction made {} total HTTP requests", totalRequests);

        printRequestSummary();
    }

    public void testMultipleParquetFilesFromS3() throws Exception {
        // Load the test Parquet file
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add multiple files to S3 fixture
        String[] s3Keys = { "test-data/part-00001.parquet", "test-data/part-00002.parquet", "test-data/part-00003.parquet" };

        for (String s3Key : s3Keys) {
            addBlobToFixture(s3Key, parquetData);
        }

        // Create Parquet format reader
        ParquetFormatReader formatReader = new ParquetFormatReader(blockFactory);

        clearRequestLogs();

        int totalRows = 0;

        // Read each file
        for (String s3Key : s3Keys) {
            String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
            StoragePath path = StoragePath.of(httpUrl);
            StorageObject storageObject = storageProvider.newObject(path);

            try (CloseableIterator<Page> iterator = formatReader.read(storageObject, null, 100)) {
                while (iterator.hasNext()) {
                    Page page = iterator.next();
                    totalRows += page.getPositionCount();
                }
            }
        }

        assertTrue("Should have read rows from multiple files", totalRows > 0);
        logger.info("Successfully read {} total rows from {} Parquet files on S3", totalRows, s3Keys.length);

        printRequestSummary();
    }

    public void testHttpStorageObjectMetadata() throws Exception {
        // Load the test Parquet file
        Path parquetFile = getTestParquetFile("employees.parquet");
        byte[] parquetData = Files.readAllBytes(parquetFile);

        // Add to S3 fixture
        String s3Key = "test-data/employees-metadata.parquet";
        addBlobToFixture(s3Key, parquetData);

        // Create HTTP storage path
        String httpUrl = getS3Endpoint() + "/" + S3FixtureUtils.BUCKET + "/" + S3FixtureUtils.WAREHOUSE + "/" + s3Key;
        StoragePath path = StoragePath.of(httpUrl);

        clearRequestLogs();

        // Test newObject without pre-known length
        StorageObject obj1 = storageProvider.newObject(path);
        assertTrue("Object should exist", obj1.exists());
        assertEquals("Length should match", parquetData.length, obj1.length());
        // Note: S3HttpFixture may not set Last-Modified header, so we just check it doesn't throw
        obj1.lastModified(); // Should not throw

        // Should have made HEAD_OBJECT request
        assertTrue("Should have made HEAD_OBJECT request", getRequestCount("HEAD_OBJECT") > 0);

        clearRequestLogs();

        // Test newObject with pre-known length (avoids HEAD request)
        StorageObject obj2 = storageProvider.newObject(path, parquetData.length);
        assertEquals("Length should match pre-known value", parquetData.length, obj2.length());

        // Should NOT have made HEAD_OBJECT request (length was pre-known)
        assertEquals("Should not make HEAD_OBJECT with pre-known length", 0, getRequestCount("HEAD_OBJECT"));

        printRequestSummary();
    }

    /**
     * Gets the path to a test Parquet file from resources.
     */
    private Path getTestParquetFile(String filename) throws IOException {
        // Try multiple possible locations
        String[] possiblePaths = {
            "src/test/resources/iceberg-fixtures/standalone/" + filename,
            "src/test/resources/org/elasticsearch/xpack/esql/iceberg/testdata/" + filename };

        for (String pathStr : possiblePaths) {
            Path path = Paths.get(pathStr);
            if (Files.exists(path)) {
                return path;
            }
        }

        // If not found in file system, try to load from classpath
        var resource = getClass().getClassLoader().getResource("iceberg-fixtures/standalone/" + filename);
        if (resource == null) {
            resource = getClass().getClassLoader().getResource("org/elasticsearch/xpack/esql/iceberg/testdata/" + filename);
        }

        if (resource != null) {
            return Paths.get(resource.getPath());
        }

        throw new IOException("Test Parquet file not found: " + filename + ". Tried paths: " + String.join(", ", possiblePaths));
    }
}
