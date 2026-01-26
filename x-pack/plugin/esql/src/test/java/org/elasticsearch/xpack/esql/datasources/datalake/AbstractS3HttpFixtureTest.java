/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.IcebergS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.S3RequestLog;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.datalake.S3FixtureUtils.WAREHOUSE;

/**
 * Base test class for Iceberg unit tests using S3HttpFixture.
 * Extends {@link ESTestCase} for standard JUnit unit testing.
 * <p>
 * <b>For integration tests:</b> Use {@code IcebergSpecTestCase} (in qa/server/iceberg module) instead,
 * which extends {@code EsqlSpecTestCase} and provides full ES cluster integration.
 * <p>
 * This class provides S3 fixture setup and Hadoop configuration for Iceberg catalog access in unit tests.
 * Tests extending this class can use the S3HttpFixture to mock S3 operations without requiring Docker
 * or actual AWS credentials.
 * <p>
 * <b>Example usage:</b>
 * <pre>{@code
 * public class MyIcebergTest extends AbstractS3HttpFixtureTest {
 *     
 *     @After
 *     public void printLogs() {
 *         // Print detailed S3 request analysis after each test
 *         printRequestSummary();
 *     }
 *     
 *     public void testReadIcebergTable() throws Exception {
 *         // Clear logs from previous tests
 *         clearRequestLogs();
 *         
 *         // Pre-populate S3 fixture with test data
 *         String metadataJson = "{\"format-version\":2,...}";
 *         addBlobToFixture("db/table/metadata/v1.metadata.json", metadataJson);
 *         
 *         // Create Iceberg catalog
 *         Catalog catalog = createCatalog();
 *         
 *         // Use catalog to access table
 *         TableIdentifier tableId = TableIdentifier.of("db", "table");
 *         Table table = catalog.loadTable(tableId);
 *         
 *         // Verify table operations work
 *         assertNotNull(table);
 *         
 *         // Note: If any unsupported S3 operations are used, the test will automatically fail
 *         // with an UnsupportedOperationException, making gap detection mandatory.
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>S3HttpFixture runs in the same JVM (no Docker required)</li>
 *   <li>Hadoop configuration pre-configured for mock S3 access</li>
 *   <li>Helper methods to pre-populate test data</li>
 *   <li>Path-style S3 access enabled for compatibility</li>
 *   <li>SSL disabled for HTTP endpoints</li>
 *   <li><b>Automatic S3 request logging to identify missing operations</b></li>
 * </ul>
 * <p>
 * <b>Request Logging and Gap Detection:</b>
 * All S3 requests are automatically logged with details including:
 * <ul>
 *   <li>HTTP method and path</li>
 *   <li>Request type (GET_OBJECT, HEAD_OBJECT, LIST_OBJECTS, etc.)</li>
 *   <li>Query parameters and headers (Range, If-Match, etc.)</li>
 *   <li>Timestamp of request</li>
 * </ul>
 * Use {@link #printRequestSummary()} to see a detailed analysis of all S3 operations.
 * <p>
 * <b>Mandatory Gap Checking:</b>
 * If an UNKNOWN request type is detected (indicating an S3 operation not explicitly supported by S3HttpHandler),
 * the test will immediately fail with an {@link UnsupportedOperationException}. This ensures that any gaps in
 * S3HttpHandler support are caught early and must be addressed before the test can pass. The exception message
 * includes details about the unsupported operation to aid in debugging.
 * <p>
 * See the <a href="../docs/s3-request-logging.md">S3 Request Logging documentation</a> for more details.
 * <p>
 * <b>Architecture:</b> This class uses {@link S3FixtureUtils} for common S3 fixture infrastructure
 * that is shared between unit tests and integration tests. The S3HttpFixture and all utility methods
 * are compatible with both test contexts through this shared utility class.
 *
 * @see S3FixtureUtils for shared S3 fixture utilities
 */
public abstract class AbstractS3HttpFixtureTest extends ESTestCase {

    protected static final Logger logger = LogManager.getLogger(AbstractS3HttpFixtureTest.class);

    @ClassRule
    public static IcebergS3HttpFixture s3Fixture = new IcebergS3HttpFixture();

    /**
     * Load fixtures from src/test/resources/iceberg-fixtures/ into the S3 fixture.
     * This runs once before all tests, making pre-built test data available automatically.
     */
    @BeforeClass
    public static void loadFixtures() {
        s3Fixture.loadFixturesFromResources();
    }
    
    /**
     * Automatically checks for unsupported S3 operations after each test.
     * If any UNKNOWN request types are detected, fails the test with a detailed error message.
     * This ensures gaps in S3HttpHandler support are caught immediately rather than silently passing.
     * 
     * <p>Subclasses can override this method if they need custom behavior, but should
     * typically call {@code super.checkForUnsupportedOperations()} to maintain gap detection.
     * 
     * @throws AssertionError if unsupported operations were detected
     */
    @org.junit.After
    public void checkForUnsupportedOperations() {
        String errorMessage = S3FixtureUtils.buildUnsupportedOperationsError();
        if (errorMessage != null) {
            fail(errorMessage);
        }
    }

    /**
     * Creates an S3FileIO configured to use the S3HttpFixture.
     * This replaces the previous Hadoop-based approach.
     *
     * @return configured S3FileIO
     */
    protected org.apache.iceberg.aws.s3.S3FileIO createS3FileIO() {
        return S3FixtureUtils.createS3FileIO(s3Fixture.getAddress());
    }

    /**
     * Adds a blob to the S3 fixture's internal storage.
     * Useful for pre-populating test data for read-only Iceberg operations.
     *
     * @param key the S3 object key (path within the bucket/warehouse)
     * @param content the content to store
     */
    protected void addBlobToFixture(String key, String content) {
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), key, content);
    }

    /**
     * Adds a blob to the S3 fixture's internal storage.
     * Useful for pre-populating test data for read-only Iceberg operations.
     *
     * @param key the S3 object key (path within the bucket/warehouse)
     * @param content the byte content to store
     */
    protected void addBlobToFixture(String key, byte[] content) {
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), key, content);
    }

    /**
     * Gets the warehouse path for use in Iceberg table locations.
     *
     * @return the S3 warehouse path
     */
    protected String getWarehousePath() {
        return S3FixtureUtils.getWarehousePath();
    }

    /**
     * Gets the S3 endpoint URL from the fixture.
     *
     * @return the S3 fixture endpoint URL
     */
    protected String getS3Endpoint() {
        return s3Fixture.getAddress();
    }
    
    /**
     * Gets all S3 requests logged.
     *
     * @return list of logged S3 requests
     */
    protected List<S3RequestLog> getRequestLogs() {
        return S3FixtureUtils.getRequestLogs();
    }
    
    /**
     * Clears request logs.
     * Useful to call in @Before methods to isolate test logs.
     */
    protected void clearRequestLogs() {
        S3FixtureUtils.clearRequestLogs();
    }
    
    /**
     * Prints a summary of all S3 requests logged.
     * Groups requests by type and shows counts.
     */
    protected void printRequestSummary() {
        S3FixtureUtils.printRequestSummary();
    }
    
    /**
     * Gets the count of requests by type.
     *
     * @param requestType the request type to count (e.g., "GET_OBJECT", "HEAD_OBJECT")
     * @return count of requests matching the type
     */
    protected int getRequestCount(String requestType) {
        return S3FixtureUtils.getRequestCount(requestType);
    }
    
    /**
     * Gets all requests matching a specific type.
     *
     * @param requestType the request type to filter by
     * @return list of matching requests
     */
    protected List<S3RequestLog> getRequestsByType(String requestType) {
        return S3FixtureUtils.getRequestsByType(requestType);
    }
    
    /**
     * Checks if there are any UNKNOWN request types, which may indicate gaps in S3HttpHandler support.
     *
     * @return true if unknown requests were detected
     */
    protected boolean hasUnknownRequests() {
        return S3FixtureUtils.hasUnknownRequests();
    }
    
    /**
     * Gets all UNKNOWN requests for debugging S3HttpHandler gaps.
     *
     * @return list of unrecognized S3 requests
     */
    protected List<S3RequestLog> getUnknownRequests() {
        return S3FixtureUtils.getUnknownRequests();
    }
    
    /**
     * Wrapper for HttpResponse that provides convenient access to response data.
     */
    protected static class HttpConnection {
        private final HttpResponse<byte[]> response;
        private final int statusCode;
        
        private HttpConnection(HttpResponse<byte[]> response) {
            this.response = response;
            this.statusCode = response.statusCode();
        }
        
        public int getResponseCode() {
            return statusCode;
        }
        
        public InputStream getInputStream() {
            return new java.io.ByteArrayInputStream(response.body());
        }
        
        public Map<String, List<String>> getHeaderFields() {
            return response.headers().map();
        }
        
        public byte[] getBody() {
            return response.body();
        }
    }
    
    /**
     * Creates and sends an HTTP request for S3 operations.
     * Handles common setup including authorization and caching headers.
     * Uses HttpClient with try-with-resources for proper cleanup.
     * 
     * @param url the full URL to connect to
     * @param method the HTTP method (GET, POST, HEAD, PUT, DELETE, etc.)
     * @return HttpConnection with the response data
     * @throws IOException if the request fails
     * @throws InterruptedException if the request is interrupted
     */
    protected HttpConnection createS3Connection(String url, String method) throws IOException, InterruptedException {
        return createS3Connection(url, method, null, null);
    }
    
    /**
     * Creates and sends an HTTP request for S3 operations with custom headers.
     * Handles common setup including authorization and caching headers.
     * Uses HttpClient with try-with-resources for proper cleanup.
     * 
     * @param url the full URL to connect to
     * @param method the HTTP method (GET, POST, HEAD, PUT, DELETE, etc.)
     * @param additionalHeaders optional map of additional headers to set (can be null)
     * @return HttpConnection with the response data
     * @throws IOException if the request fails
     * @throws InterruptedException if the request is interrupted
     */
    protected HttpConnection createS3Connection(String url, String method, Map<String, String> additionalHeaders) 
            throws IOException, InterruptedException {
        return createS3Connection(url, method, additionalHeaders, null);
    }
    
    /**
     * Creates and sends an HTTP request for S3 operations with custom headers and body.
     * Handles common setup including authorization and caching headers.
     * Uses HttpClient with try-with-resources for proper cleanup (Java 21+).
     * 
     * @param url the full URL to connect to
     * @param method the HTTP method (GET, POST, HEAD, PUT, DELETE, etc.)
     * @param additionalHeaders optional map of additional headers to set (can be null)
     * @param body optional request body (can be null)
     * @return HttpConnection with the response data
     * @throws IOException if the request fails
     * @throws InterruptedException if the request is interrupted
     */
    protected HttpConnection createS3Connection(String url, String method, Map<String, String> additionalHeaders, byte[] body) 
            throws IOException, InterruptedException {
        URI uri = URI.create(url);
        
        // Build the request
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(Duration.ofSeconds(10));
        
        // Set the HTTP method and body
        if (body != null && body.length > 0) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofByteArray(body));
        } else if ("POST".equals(method) || "PUT".equals(method)) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        
        // Add authorization header
        requestBuilder.header("Authorization", createS3AuthHeader(method, uri.getPath()));
        
        // Disable caching explicitly
        requestBuilder.header("Cache-Control", "no-cache, no-store, must-revalidate");
        requestBuilder.header("Pragma", "no-cache");
        requestBuilder.header("Expires", "0");
        
        // Add any additional headers
        if (additionalHeaders != null) {
            for (Map.Entry<String, String> header : additionalHeaders.entrySet()) {
                requestBuilder.header(header.getKey(), header.getValue());
            }
        }
        
        // Create HttpClient and send request using try-with-resources (Java 21+ AutoCloseable support)
        try (HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()) {
            
            HttpRequest request = requestBuilder.build();
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            
            return new HttpConnection(response);
        }
    }
    
    /**
     * Creates an AWS Signature Version 4 style authorization header for S3 requests.
     * This is a simplified version for testing - real S3 uses full SigV4.
     * 
     * @param method the HTTP method
     * @param path the request path
     * @return authorization header value
     */
    protected String createS3AuthHeader(String method, String path) {
        return "AWS4-HMAC-SHA256 Credential=" + ACCESS_KEY + "/20260126/us-east-1/s3/aws4_request, "
            + "SignedHeaders=host;x-amz-date, Signature=dummy";
    }
}
