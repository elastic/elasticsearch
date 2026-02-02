/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datalake;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.apache.iceberg.aws.s3.S3FileIO;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.test.fixture.HttpHeaderParser;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.checkAuthorization;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

/**
 * Shared utilities for Iceberg tests using S3HttpFixture.
 * <p>
 * This class provides reusable components that make the S3HttpFixture infrastructure compatible
 * with both unit tests and integration tests, avoiding code duplication.
 * <p>
 * <b>Architecture:</b>
 * <ul>
 *   <li><b>Unit Tests:</b> {@link org.elasticsearch.xpack.esql.datasources.datalake.AbstractS3HttpFixtureTest}
 *       extends {@code ESTestCase} and uses this utility class for S3 fixture setup</li>
 *   <li><b>Integration Tests:</b> {@code IcebergSpecTestCase} (in qa/server/iceberg module)
 *       extends {@code EsqlSpecTestCase} (which extends {@code ESRestTestCase}) and uses this utility class
 *       for the same S3 fixture setup</li>
 * </ul>
 * <p>
 * By extracting common functionality into this utility class, we enable the S3HttpFixture to work
 * seamlessly in both test contexts:
 * <ul>
 *   <li>Unit tests: Fast, in-memory testing with mock S3 (no ES cluster required)</li>
 *   <li>Integration tests: End-to-end testing with real ES cluster + S3HttpFixture</li>
 * </ul>
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>S3HttpFixture with automatic request logging</li>
 *   <li>S3FileIO configuration for mock S3 access (no Hadoop dependencies)</li>
 *   <li>Direct Iceberg table access using StaticTableOperations</li>
 *   <li>Request logging for debugging S3 operations</li>
 *   <li>Gap detection for unsupported S3 operations</li>
 *   <li>Automatic loading of fixtures from {@code src/test/resources/iceberg-fixtures/}</li>
 * </ul>
 *
 * @see org.elasticsearch.xpack.esql.datasources.datalake.AbstractS3HttpFixtureTest for unit tests
 */
public final class S3FixtureUtils {

    private static final Logger logger = LogManager.getLogger(S3FixtureUtils.class);

    public static final String BUCKET = "iceberg-test";
    public static final String WAREHOUSE = "warehouse";
    public static final String ACCESS_KEY = "test-access-key";
    public static final String SECRET_KEY = "test-secret-key";

    /**
     * Thread-safe storage for S3 request logs.
     * Each entry captures details about an S3 request for analysis.
     */
    private static final List<S3RequestLog> requestLogs = Collections.synchronizedList(new ArrayList<>());

    private S3FixtureUtils() {
        // Utility class
    }

    /**
     * Represents a logged S3 request with all relevant details.
     */
    public static class S3RequestLog {
        private final String method;
        private final String path;
        private final String queryString;
        private final Map<String, List<String>> headers;
        private final Map<String, List<String>> queryParams;
        private final long timestamp;
        private final String requestType;

        public S3RequestLog(
            String method,
            String path,
            String queryString,
            Map<String, List<String>> headers,
            Map<String, List<String>> queryParams,
            String requestType
        ) {
            this.method = method;
            this.path = path;
            this.queryString = queryString != null ? queryString : "";
            this.headers = new HashMap<>(headers);
            this.queryParams = new HashMap<>(queryParams);
            this.timestamp = System.currentTimeMillis();
            this.requestType = requestType;
        }

        public String getMethod() {
            return method;
        }

        public String getPath() {
            return path;
        }

        public String getQueryString() {
            return queryString;
        }

        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        public Map<String, List<String>> getQueryParams() {
            return queryParams;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getRequestType() {
            return requestType;
        }

        private static final String[] INTERESTING_HEADERS = { "Range", "If-Match", "If-None-Match", "X-amz-copy-source" };

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(method).append(" ").append(path);
            if (queryString.isEmpty() == false) {
                sb.append("?").append(queryString);
            }
            sb.append(" [").append(requestType).append("]");

            // Include interesting headers
            for (String headerName : INTERESTING_HEADERS) {
                if (headers.containsKey(headerName)) {
                    sb.append(" ").append(headerName).append(": ").append(headers.get(headerName));
                }
            }

            return sb.toString();
        }
    }

    /**
     * Custom S3HttpFixture that exposes the handler for pre-populating test data.
     * Automatically loads files from src/test/resources/iceberg-fixtures/ on startup.
     */
    public static class IcebergS3HttpFixture extends S3HttpFixture {
        private S3HttpHandler handler;
        private final S3ConsistencyModel consistencyModel;
        private final int fixedPort;

        // Server and executor for fixed-port mode (need our own since parent's are private)
        private HttpServer server;
        private ExecutorService executorService;

        public IcebergS3HttpFixture() {
            this(S3ConsistencyModel.AWS_DEFAULT, 0);
        }

        /**
         * Creates an S3 HTTP fixture with a fixed port.
         *
         * @param fixedPort the port to bind to, or 0 for a random available port
         */
        public IcebergS3HttpFixture(int fixedPort) {
            this(S3ConsistencyModel.AWS_DEFAULT, fixedPort);
        }

        public IcebergS3HttpFixture(S3ConsistencyModel consistencyModel) {
            this(consistencyModel, 0);
        }

        public IcebergS3HttpFixture(S3ConsistencyModel consistencyModel, int fixedPort) {
            super(true, BUCKET, WAREHOUSE, () -> consistencyModel, fixedAccessKey(ACCESS_KEY, ANY_REGION, "s3"));
            this.consistencyModel = consistencyModel;
            this.fixedPort = fixedPort;
        }

        /**
         * Returns the port to use for the fixture.
         * Can be overridden to use a fixed port instead of a random one.
         */
        protected int getPort() {
            return fixedPort;
        }

        /**
         * Starts the S3 HTTP fixture server.
         * This is a public wrapper around the protected before() method for use outside of JUnit rules.
         */
        public void start() {
            try {
                before();
            } catch (Throwable e) {
                throw new RuntimeException("Failed to start S3HttpFixture", e);
            }
        }

        /**
         * Stops the S3 HTTP fixture server.
         * This is a public wrapper around the protected after() method for use outside of JUnit rules.
         */
        public void shutdown() {
            after();
        }

        @Override
        protected void before() throws Throwable {
            if (fixedPort > 0) {
                // Use fixed port - we manage our own server
                this.executorService = EsExecutors.newScaling(
                    "s3-http-fixture",
                    1,
                    100,
                    30,
                    TimeUnit.SECONDS,
                    true,
                    TestEsExecutors.testOnlyDaemonThreadFactory("s3-http-fixture"),
                    new ThreadContext(Settings.EMPTY)
                );

                InetSocketAddress address = new InetSocketAddress(InetAddress.getByName("localhost"), fixedPort);
                this.server = HttpServer.create(address, 0);
                this.server.createContext("/", Objects.requireNonNull(createHandler()));
                this.server.setExecutor(executorService);
                server.start();
                logger.info("running IcebergS3HttpFixture at " + getAddress() + " (fixed port)");
            } else {
                // Use random port - delegate to parent
                super.before();
            }
        }

        @Override
        protected void after() {
            if (fixedPort > 0 && server != null) {
                // We manage our own server
                server.stop(0);
                ThreadPool.terminate(executorService, 10, TimeUnit.SECONDS);
            } else {
                // Delegate to parent
                super.after();
            }
        }

        @Override
        public String getAddress() {
            if (fixedPort > 0 && server != null) {
                return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
            }
            return super.getAddress();
        }

        @Override
        protected HttpHandler createHandler() {
            handler = new S3HttpHandler(BUCKET, WAREHOUSE, consistencyModel) {
                @Override
                public void handle(final HttpExchange exchange) throws IOException {
                    // Log the request BEFORE processing
                    logRequest(exchange);

                    try {
                        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");

                        if (authHeader != null && authHeader.startsWith("AWS4-HMAC-SHA256")) {
                            // S3 request with AWS auth - use normal flow
                            if (checkAuthorization(fixedAccessKey(ACCESS_KEY, ANY_REGION, "s3"), exchange)) {
                                super.handle(exchange);
                            }
                        } else {
                            // Plain HTTP request - serve directly without auth
                            handlePlainHttpRequest(exchange);
                        }
                    } catch (Error e) {
                        org.elasticsearch.ExceptionsHelper.maybeDieOnAnotherThread(e);
                        throw e;
                    }
                }

                /**
                 * Logs detailed information about an S3 request for debugging and gap analysis.
                 * This method is defensive - parsing errors are caught and don't break request handling.
                 */
                private void logRequest(HttpExchange exchange) {
                    String method = exchange.getRequestMethod();
                    String path = exchange.getRequestURI().getPath();
                    String queryString = exchange.getRequestURI().getQuery();
                    Map<String, List<String>> headers = new HashMap<>(exchange.getRequestHeaders());

                    // Parse the request using the parent's method to get request type
                    // Wrap in try-catch to prevent parsing errors from breaking request handling
                    String requestType = "UNKNOWN";
                    Map<String, List<String>> queryParameters = Collections.emptyMap();
                    try {
                        S3Request request = parseRequest(exchange);
                        requestType = determineRequestType(request);
                        queryParameters = request.queryParameters();
                    } catch (Exception e) {
                        // If parsing fails, log but continue with UNKNOWN type
                        logger.debug("Failed to parse S3 request for logging: {}", e.getMessage());
                    }

                    S3RequestLog log = new S3RequestLog(method, path, queryString, headers, queryParameters, requestType);

                    // Store in global list
                    requestLogs.add(log);

                    // Log to console for immediate visibility
                    logger.info("S3 Request: {} {} {}", method, path + (queryString != null ? "?" + queryString : ""), requestType);
                }

                /**
                 * Handles plain HTTP requests without AWS authentication.
                 * Supports GET, HEAD, and Range requests for serving files over plain HTTP.
                 * The exchange is always closed in the finally block to ensure proper resource cleanup.
                 */
                private void handlePlainHttpRequest(HttpExchange exchange) throws IOException {
                    try {
                        String method = exchange.getRequestMethod();
                        String path = exchange.getRequestURI().getPath();
                        logger.info("HTTP request: {} {} (looking up blob with path: {})", method, exchange.getRequestURI(), path);

                        BytesReference blob = blobs().get(path);

                        if (blob == null) {
                            logger.warn("Blob not found for HTTP path: {} (available blob keys: {})", path, blobs().size());
                            // Log first few blob keys for debugging
                            blobs().keySet().stream().limit(10).forEach(k -> logger.warn("  Available blob key: {}", k));
                            exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                            return;
                        }

                        logger.info("Found blob for HTTP path: {} ({} bytes)", path, blob.length());

                        String contentType = guessContentType(path);

                        if ("HEAD".equals(method)) {
                            exchange.getResponseHeaders().add("Content-Length", String.valueOf(blob.length()));
                            exchange.getResponseHeaders().add("Content-Type", contentType);
                            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
                            return;
                        }

                        if ("GET".equals(method)) {
                            String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                            if (rangeHeader == null) {
                                // Full content
                                exchange.getResponseHeaders().add("Content-Type", contentType);
                                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                                blob.writeTo(exchange.getResponseBody());
                            } else {
                                // Range request - parse and return partial content
                                HttpHeaderParser.Range range = HttpHeaderParser.parseRangeHeader(rangeHeader);
                                if (range == null) {
                                    // Invalid range header - return full content
                                    exchange.getResponseHeaders().add("Content-Type", contentType);
                                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                                    blob.writeTo(exchange.getResponseBody());
                                    return;
                                }

                                long start = range.start();
                                // For open-ended ranges (bytes=N-), end is null, meaning "to end of file"
                                long end = range.end() != null ? range.end() : blob.length() - 1;

                                if (end < start) {
                                    // Invalid range - return full content
                                    exchange.getResponseHeaders().add("Content-Type", contentType);
                                    exchange.sendResponseHeaders(RestStatus.OK.getStatus(), blob.length());
                                    blob.writeTo(exchange.getResponseBody());
                                    return;
                                } else if (blob.length() <= start) {
                                    // Start beyond file size - range not satisfiable
                                    exchange.getResponseHeaders().add("Content-Type", contentType);
                                    exchange.sendResponseHeaders(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus(), -1);
                                    return;
                                }

                                // Calculate the actual slice to return
                                var responseBlob = blob.slice(
                                    Math.toIntExact(start),
                                    Math.toIntExact(Math.min(end - start + 1, blob.length() - start))
                                );
                                end = start + responseBlob.length() - 1;

                                exchange.getResponseHeaders().add("Content-Type", contentType);
                                exchange.getResponseHeaders()
                                    .add(
                                        "Content-Range",
                                        String.format(java.util.Locale.ROOT, "bytes %d-%d/%d", start, end, blob.length())
                                    );
                                exchange.sendResponseHeaders(RestStatus.PARTIAL_CONTENT.getStatus(), responseBlob.length());
                                responseBlob.writeTo(exchange.getResponseBody());
                            }
                            return;
                        }

                        // Unsupported method
                        exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), -1);
                    } finally {
                        exchange.close();
                    }
                }

                /**
                 * Guesses the Content-Type header based on file extension.
                 */
                private String guessContentType(String path) {
                    if (path.endsWith(".csv")) return "text/csv";
                    if (path.endsWith(".parquet")) return "application/octet-stream";
                    if (path.endsWith(".json")) return "application/json";
                    if (path.endsWith(".avro")) return "application/avro";
                    if (path.endsWith(".orc")) return "application/octet-stream";
                    return "application/octet-stream";
                }

                /**
                 * Determines the type of S3 request for logging purposes.
                 */
                private String determineRequestType(S3Request request) {
                    if (request.isHeadObjectRequest()) return "HEAD_OBJECT";
                    if (request.isGetObjectRequest()) return "GET_OBJECT";
                    if (request.isPutObjectRequest()) return "PUT_OBJECT";
                    if (request.isDeleteObjectRequest()) return "DELETE_OBJECT";
                    if (request.isListObjectsRequest()) return "LIST_OBJECTS";
                    if (request.isMultiObjectDeleteRequest()) return "MULTI_DELETE";
                    if (request.isInitiateMultipartUploadRequest()) return "INITIATE_MULTIPART";
                    if (request.isUploadPartRequest()) return "UPLOAD_PART";
                    if (request.isCompleteMultipartUploadRequest()) return "COMPLETE_MULTIPART";
                    if (request.isAbortMultipartUploadRequest()) return "ABORT_MULTIPART";
                    if (request.isListMultipartUploadsRequest()) return "LIST_MULTIPART_UPLOADS";
                    return "UNKNOWN";
                }
            };
            return handler;
        }

        public S3HttpHandler getHandler() {
            return handler;
        }

        /**
         * Loads all files from src/test/resources/iceberg-fixtures/ into the S3 fixture's blob storage.
         * This allows tests to use pre-built Iceberg metadata and Parquet files without manual setup.
         *
         * Files are mapped to S3 paths preserving their directory structure:
         * - src/test/resources/iceberg-fixtures/db/table/metadata/v1.json → s3://bucket/warehouse/db/table/metadata/v1.json
         */
        public void loadFixturesFromResources() {
            try {
                // Try to find iceberg-fixtures using the context class loader first (for test modules)
                // Fall back to this class's classloader if not found
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                URL resourceUrl = classLoader.getResource("iceberg-fixtures");

                if (resourceUrl == null) {
                    // Try this class's classloader
                    classLoader = getClass().getClassLoader();
                    resourceUrl = classLoader.getResource("iceberg-fixtures");
                }

                if (resourceUrl == null) {
                    logger.info("No iceberg-fixtures directory found in resources, skipping fixture loading");
                    return;
                }

                URI resourceUri = resourceUrl.toURI();
                Path fixturesPath;

                // Handle both file system and JAR resources
                if (resourceUri.getScheme().equals("jar")) {
                    // Running from JAR - need to use FileSystem
                    FileSystem fs = FileSystems.newFileSystem(resourceUri, Collections.emptyMap());
                    fixturesPath = fs.getPath("/iceberg-fixtures");
                } else {
                    // Running from file system
                    fixturesPath = Paths.get(resourceUri);
                }

                if (Files.exists(fixturesPath) == false) {
                    logger.info("iceberg-fixtures path does not exist: {}", fixturesPath);
                    return;
                }

                // Ensure handler is available before loading fixtures
                if (handler == null) {
                    logger.warn("Handler is null when loading fixtures - fixture may not be started yet");
                    // Try to wait a bit for handler to be initialized
                    for (int i = 0; i < 10 && handler == null; i++) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    if (handler == null) {
                        logger.error("Handler is still null after waiting - cannot load fixtures");
                        return;
                    }
                }

                // Walk the directory tree and load all files
                final int[] loadedCount = { 0 };
                try (Stream<Path> paths = Files.walk(fixturesPath)) {
                    paths.filter(Files::isRegularFile).forEach(filePath -> {
                        try {
                            // Get relative path from fixtures root
                            Path relativePath = fixturesPath.relativize(filePath);
                            String s3Key = relativePath.toString().replace('\\', '/'); // Normalize path separators

                            // Read file contents
                            byte[] content = Files.readAllBytes(filePath);

                            // Add to S3 fixture
                            String fullPath = "/" + BUCKET + "/" + WAREHOUSE + "/" + s3Key;
                            handler.blobs().put(fullPath, new BytesArray(content));
                            loadedCount[0]++;

                            logger.info("Loaded fixture: {} → {} ({} bytes)", s3Key, fullPath, content.length);
                        } catch (IOException e) {
                            logger.error("Failed to load fixture file: {}", filePath, e);
                        }
                    });
                }

                logger.info("Loaded {} fixtures from iceberg-fixtures directory into blob storage", loadedCount[0]);
            } catch (IOException | URISyntaxException e) {
                logger.error("Failed to load fixtures from resources", e);
                throw new RuntimeException("Failed to load iceberg-fixtures", e);
            }
        }

        /**
         * Adds content to the fixture's blob storage for plain HTTP access.
         * Can be used by any test to serve files over HTTP.
         *
         * @param path the path (e.g., "/test/data.csv")
         * @param content the content as a byte array
         */
        public void addContent(String path, byte[] content) {
            handler.blobs().put(path, new BytesArray(content));
        }

        /**
         * Adds content to the fixture's blob storage for plain HTTP access.
         * Can be used by any test to serve files over HTTP.
         *
         * @param path the path (e.g., "/test/data.csv")
         * @param content the content as a string
         */
        public void addContent(String path, String content) {
            addContent(path, content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Creates an S3FileIO configured to use the S3HttpFixture.
     * This replaces the previous HadoopCatalog-based approach, eliminating Hadoop dependencies.
     *
     * @param s3Endpoint the S3 fixture endpoint URL
     * @return configured S3FileIO instance
     */
    public static S3FileIO createS3FileIO(String s3Endpoint) {
        org.elasticsearch.xpack.esql.datasources.s3.S3Configuration s3Config = org.elasticsearch.xpack.esql.datasources.s3.S3Configuration
            .fromFields(
                ACCESS_KEY,
                SECRET_KEY,
                s3Endpoint,
                null // region
            );
        return org.elasticsearch.xpack.esql.datasources.s3.S3FileIOFactory.create(s3Config);
    }

    /**
     * Adds a blob to the S3 fixture's internal storage.
     * Useful for pre-populating test data for read-only Iceberg operations.
     *
     * @param handler the S3HttpHandler to add the blob to
     * @param key the S3 object key (path within the bucket/warehouse)
     * @param content the content to store
     */
    public static void addBlobToFixture(S3HttpHandler handler, String key, String content) {
        String fullPath = "/" + BUCKET + "/" + WAREHOUSE + "/" + key;
        handler.blobs().put(fullPath, new BytesArray(content.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Adds a blob to the S3 fixture's internal storage.
     * Useful for pre-populating test data for read-only Iceberg operations.
     *
     * @param handler the S3HttpHandler to add the blob to
     * @param key the S3 object key (path within the bucket/warehouse)
     * @param content the byte content to store
     */
    public static void addBlobToFixture(S3HttpHandler handler, String key, byte[] content) {
        String fullPath = "/" + BUCKET + "/" + WAREHOUSE + "/" + key;
        handler.blobs().put(fullPath, new BytesArray(content));
    }

    /**
     * Gets the warehouse path for use in Iceberg table locations.
     * Uses S3 URI scheme (s3://) instead of Hadoop's S3A scheme (s3a://).
     *
     * @return the S3 warehouse path
     */
    public static String getWarehousePath() {
        return "s3://" + BUCKET + "/" + WAREHOUSE;
    }

    /**
     * Gets all S3 requests logged.
     *
     * @return list of logged S3 requests
     */
    public static List<S3RequestLog> getRequestLogs() {
        return new ArrayList<>(requestLogs);
    }

    /**
     * Clears request logs.
     * Useful to call in @Before methods to isolate test logs.
     */
    public static void clearRequestLogs() {
        requestLogs.clear();
    }

    /**
     * Prints a summary of all S3 requests logged.
     * Groups requests by type and shows counts.
     */
    public static void printRequestSummary() {
        List<S3RequestLog> logs = getRequestLogs();

        if (logs.isEmpty()) {
            logger.info("No S3 requests logged");
            return;
        }

        logger.info("=== S3 Request Summary ===");
        logger.info("Total requests: {}", logs.size());

        // Group by request type
        Map<String, List<S3RequestLog>> byType = logs.stream().collect(Collectors.groupingBy(S3RequestLog::getRequestType));

        logger.info("\nRequests by type:");
        byType.entrySet()
            .stream()
            .sorted(Map.Entry.<String, List<S3RequestLog>>comparingByValue((a, b) -> Integer.compare(b.size(), a.size())))
            .forEach(entry -> {
                logger.info("  {}: {} requests", entry.getKey(), entry.getValue().size());
            });

        logger.info("\nDetailed request log:");
        for (int i = 0; i < logs.size(); i++) {
            S3RequestLog log = logs.get(i);
            logger.info("  {}: {}", i + 1, log);
        }

        logger.info("\nUnique paths accessed:");
        logs.stream().map(S3RequestLog::getPath).distinct().sorted().forEach(path -> logger.info("  {}", path));

        // Check for UNKNOWN request types (potential gaps)
        List<S3RequestLog> unknownRequests = logs.stream()
            .filter(log -> "UNKNOWN".equals(log.getRequestType()))
            .collect(Collectors.toList());

        if (unknownRequests.isEmpty() == false) {
            logger.warn("\n!!! WARNING: {} UNKNOWN request types detected !!!", unknownRequests.size());
            logger.warn("These may indicate missing S3 operations in S3HttpHandler:");
            unknownRequests.forEach(log -> logger.warn("  {}", log));
        }

        logger.info("=== End S3 Request Summary ===\n");
    }

    /**
     * Gets the count of requests by type.
     *
     * @param requestType the request type to count (e.g., "GET_OBJECT", "HEAD_OBJECT")
     * @return count of requests matching the type
     */
    public static int getRequestCount(String requestType) {
        return (int) getRequestLogs().stream().filter(log -> requestType.equals(log.getRequestType())).count();
    }

    /**
     * Gets all requests matching a specific type.
     *
     * @param requestType the request type to filter by
     * @return list of matching requests
     */
    public static List<S3RequestLog> getRequestsByType(String requestType) {
        return getRequestLogs().stream().filter(log -> requestType.equals(log.getRequestType())).collect(Collectors.toList());
    }

    /**
     * Checks if there are any UNKNOWN request types, which may indicate gaps in S3HttpHandler support.
     *
     * @return true if unknown requests were detected
     */
    public static boolean hasUnknownRequests() {
        return getRequestLogs().stream().anyMatch(log -> "UNKNOWN".equals(log.getRequestType()));
    }

    /**
     * Gets all UNKNOWN requests for debugging S3HttpHandler gaps.
     *
     * @return list of unrecognized S3 requests
     */
    public static List<S3RequestLog> getUnknownRequests() {
        return getRequestsByType("UNKNOWN");
    }

    /**
     * Checks for unsupported S3 operations and builds an error message if any are found.
     * This method should be called after test execution to ensure gaps in S3HttpHandler
     * support are caught immediately.
     *
     * @return error message describing unsupported operations, or null if none found
     */
    public static String buildUnsupportedOperationsError() {
        List<S3RequestLog> unknownRequests = getUnknownRequests();

        if (unknownRequests.isEmpty()) {
            return null;
        }

        // Build detailed error message
        StringBuilder errorMessage = new StringBuilder();
        errorMessage.append(
            String.format(
                "Detected %d unsupported S3 operation(s) during test execution. "
                    + "This indicates a gap in S3HttpHandler support that must be addressed:\n\n",
                unknownRequests.size()
            )
        );

        for (int i = 0; i < unknownRequests.size(); i++) {
            S3RequestLog log = unknownRequests.get(i);
            errorMessage.append(
                String.format(
                    "  %d. %s %s%s\n",
                    i + 1,
                    log.getMethod(),
                    log.getPath(),
                    log.getQueryString().isEmpty() ? "" : "?" + log.getQueryString()
                )
            );

            // Add interesting headers
            if (log.getHeaders().isEmpty() == false) {
                log.getHeaders()
                    .entrySet()
                    .stream()
                    .filter(
                        e -> e.getKey() != null
                            && (e.getKey().equals("Range")
                                || e.getKey().equals("If-Match")
                                || e.getKey().equals("If-None-Match")
                                || e.getKey().startsWith("x-amz-"))
                    )
                    .forEach(e -> errorMessage.append(String.format("     Header: %s = %s\n", e.getKey(), e.getValue())));
            }
        }

        errorMessage.append("\nPossible solutions:\n");
        errorMessage.append("  1. Implement support for this operation in S3HttpHandler\n");
        errorMessage.append("  2. Update Iceberg configuration to avoid this operation\n");
        errorMessage.append("  3. Pre-populate test data to avoid write operations\n");

        return errorMessage.toString();
    }
}
