/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;

/**
 * Shared utilities for S3 fixture-based integration tests.
 * Provides common S3 fixture infrastructure for testing external data sources like Iceberg and Parquet.
 */
public final class S3FixtureUtils {

    private static final Logger logger = LogManager.getLogger(S3FixtureUtils.class);

    /** Default S3 access key for test fixtures */
    public static final String ACCESS_KEY = "test-access-key";

    /** Default S3 secret key for test fixtures */
    public static final String SECRET_KEY = "test-secret-key";

    /** Default bucket name for test fixtures */
    public static final String BUCKET = "test-bucket";

    /** Default warehouse path within the bucket */
    public static final String WAREHOUSE = "warehouse";

    /** Resource path for test fixtures */
    private static final String FIXTURES_RESOURCE_PATH = "/iceberg-fixtures";

    /** Thread-safe list of S3 request logs */
    private static final CopyOnWriteArrayList<S3RequestLog> requestLogs = new CopyOnWriteArrayList<>();

    /** Set of known/expected S3 request types */
    private static final Set<String> KNOWN_REQUEST_TYPES = Set.of(
        "GET_OBJECT",
        "HEAD_OBJECT",
        "PUT_OBJECT",
        "DELETE_OBJECT",
        "LIST_OBJECTS",
        "LIST_OBJECTS_V2",
        "INITIATE_MULTIPART",
        "UPLOAD_PART",
        "COMPLETE_MULTIPART",
        "ABORT_MULTIPART",
        "LIST_MULTIPART_UPLOADS",
        "MULTI_OBJECT_DELETE"
    );

    /** Set of unsupported operations encountered during test execution */
    private static final Set<String> unsupportedOperations = ConcurrentHashMap.newKeySet();

    private S3FixtureUtils() {
        // Utility class - no instantiation
    }

    /**
     * Get the warehouse path for S3 URLs.
     */
    public static String getWarehousePath() {
        return WAREHOUSE;
    }

    /**
     * Get all recorded S3 request logs.
     */
    public static List<S3RequestLog> getRequestLogs() {
        return Collections.unmodifiableList(new ArrayList<>(requestLogs));
    }

    /**
     * Clear all recorded S3 request logs.
     */
    public static void clearRequestLogs() {
        requestLogs.clear();
        unsupportedOperations.clear();
    }

    /**
     * Print a summary of S3 requests to the logger.
     */
    public static void printRequestSummary() {
        List<S3RequestLog> logs = getRequestLogs();
        if (logs.isEmpty()) {
            logger.info("No S3 requests recorded");
            return;
        }

        Map<String, Long> byType = logs.stream().collect(Collectors.groupingBy(S3RequestLog::getRequestType, Collectors.counting()));

        logger.info("S3 Request Summary ({} total requests):", logs.size());
        byType.entrySet()
            .stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .forEach(entry -> logger.info("  {}: {}", entry.getKey(), entry.getValue()));
    }

    /**
     * Get the count of requests of a specific type.
     */
    public static int getRequestCount(String requestType) {
        return (int) requestLogs.stream().filter(log -> requestType.equals(log.getRequestType())).count();
    }

    /**
     * Get all requests of a specific type.
     */
    public static List<S3RequestLog> getRequestsByType(String requestType) {
        return requestLogs.stream().filter(log -> requestType.equals(log.getRequestType())).collect(Collectors.toList());
    }

    /**
     * Check if any unknown/unsupported request types were encountered.
     */
    public static boolean hasUnknownRequests() {
        return requestLogs.stream().anyMatch(log -> KNOWN_REQUEST_TYPES.contains(log.getRequestType()) == false);
    }

    /**
     * Get all unknown/unsupported requests.
     */
    public static List<S3RequestLog> getUnknownRequests() {
        return requestLogs.stream().filter(log -> KNOWN_REQUEST_TYPES.contains(log.getRequestType()) == false).collect(Collectors.toList());
    }

    /**
     * Build an error message for unsupported S3 operations, or null if none.
     */
    public static String buildUnsupportedOperationsError() {
        if (unsupportedOperations.isEmpty()) {
            return null;
        }
        return "Unsupported S3 operations encountered: " + String.join(", ", unsupportedOperations);
    }

    /**
     * Add a blob to the S3 fixture.
     */
    public static void addBlobToFixture(S3HttpHandler handler, String key, String content) {
        addBlobToFixture(handler, key, content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Add a blob to the S3 fixture.
     */
    public static void addBlobToFixture(S3HttpHandler handler, String key, byte[] content) {
        String fullPath = "/" + BUCKET + "/" + key;
        handler.blobs().put(fullPath, new BytesArray(content));
        logRequest("PUT_OBJECT", fullPath, content.length);
    }

    /**
     * Log an S3 request.
     */
    private static void logRequest(String requestType, String path, long contentLength) {
        requestLogs.add(new S3RequestLog(requestType, path, contentLength, System.currentTimeMillis()));
    }

    /**
     * Create an S3FileIO configured to use the S3HttpFixture.
     * This method uses reflection to avoid compile-time dependency on Iceberg.
     * The Iceberg dependencies must be on the classpath at runtime.
     *
     * @param endpoint the S3 endpoint URL
     * @return an S3FileIO instance configured for the fixture
     * @throws RuntimeException if Iceberg is not on the classpath
     */
    @SuppressWarnings("unchecked")
    public static <T extends AutoCloseable> T createS3FileIO(String endpoint) {
        return createS3FileIO(endpoint, ACCESS_KEY, SECRET_KEY);
    }

    /**
     * Create an S3FileIO with custom credentials.
     * This method uses reflection to avoid compile-time dependency on Iceberg.
     * The Iceberg dependencies must be on the classpath at runtime.
     *
     * @param endpoint the S3 endpoint URL
     * @param accessKey the S3 access key
     * @param secretKey the S3 secret key
     * @return an S3FileIO instance configured with the given credentials
     * @throws RuntimeException if Iceberg is not on the classpath
     */
    @SuppressWarnings("unchecked")
    public static <T extends AutoCloseable> T createS3FileIO(String endpoint, String accessKey, String secretKey) {
        try {
            // Use reflection to create S3FileIO to avoid compile-time dependency on Iceberg
            // This allows the qa/server module to compile without Iceberg while still
            // providing this utility for modules that have Iceberg on the classpath

            Class<?> s3FileIOClass = Class.forName("org.apache.iceberg.aws.s3.S3FileIO");
            Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
            Class<?> s3ClientBuilderClass = Class.forName("software.amazon.awssdk.services.s3.S3ClientBuilder");
            Class<?> awsBasicCredentialsClass = Class.forName("software.amazon.awssdk.auth.credentials.AwsBasicCredentials");
            Class<?> staticCredentialsProviderClass = Class.forName("software.amazon.awssdk.auth.credentials.StaticCredentialsProvider");
            Class<?> regionClass = Class.forName("software.amazon.awssdk.regions.Region");
            Class<?> urlConnectionHttpClientClass = Class.forName("software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient");
            Class<?> profileFileClass = Class.forName("software.amazon.awssdk.profiles.ProfileFile");

            // Create credentials
            Object credentials = awsBasicCredentialsClass.getMethod("create", String.class, String.class)
                .invoke(null, accessKey, secretKey);
            Object credentialsProvider = staticCredentialsProviderClass.getMethod(
                "create",
                Class.forName("software.amazon.awssdk.auth.credentials.AwsCredentials")
            ).invoke(null, credentials);

            // Get US_EAST_1 region
            Object usEast1Region = regionClass.getField("US_EAST_1").get(null);

            // Create HTTP client
            Object httpClientBuilder = urlConnectionHttpClientClass.getMethod("builder").invoke(null);
            Object httpClient = httpClientBuilder.getClass().getMethod("build").invoke(httpClientBuilder);

            // Create empty profile file
            Object profileFileBuilder = profileFileClass.getMethod("builder").invoke(null);
            Object credentialsType = Class.forName("software.amazon.awssdk.profiles.ProfileFile$Type").getField("CREDENTIALS").get(null);
            profileFileBuilder.getClass()
                .getMethod("type", Class.forName("software.amazon.awssdk.profiles.ProfileFile$Type"))
                .invoke(profileFileBuilder, credentialsType);
            profileFileBuilder.getClass()
                .getMethod("content", InputStream.class)
                .invoke(profileFileBuilder, new java.io.ByteArrayInputStream(new byte[0]));
            Object emptyProfileFile = profileFileBuilder.getClass().getMethod("build").invoke(profileFileBuilder);

            // Create S3Client using a supplier lambda
            java.util.function.Supplier<Object> s3ClientSupplier = () -> {
                try {
                    Object builder = s3ClientClass.getMethod("builder").invoke(null);

                    // Set credentials
                    builder.getClass()
                        .getMethod("credentialsProvider", Class.forName("software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"))
                        .invoke(builder, credentialsProvider);

                    // Set endpoint if provided
                    if (endpoint != null) {
                        builder.getClass().getMethod("endpointOverride", java.net.URI.class).invoke(builder, java.net.URI.create(endpoint));
                    }

                    // Set region
                    builder.getClass().getMethod("region", regionClass).invoke(builder, usEast1Region);

                    // Enable path-style access
                    builder.getClass().getMethod("forcePathStyle", Boolean.class).invoke(builder, true);

                    // Set HTTP client
                    builder.getClass()
                        .getMethod("httpClient", Class.forName("software.amazon.awssdk.http.SdkHttpClient"))
                        .invoke(builder, httpClient);

                    return builder.getClass().getMethod("build").invoke(builder);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create S3Client", e);
                }
            };

            // Create SerializableSupplier wrapper
            Class<?> serializableSupplierClass = Class.forName("org.apache.iceberg.util.SerializableSupplier");

            // Create a dynamic proxy that implements SerializableSupplier
            Object serializableSupplier = java.lang.reflect.Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class<?>[] { serializableSupplierClass, java.io.Serializable.class },
                (proxy, method, args) -> {
                    if ("get".equals(method.getName())) {
                        return s3ClientSupplier.get();
                    }
                    return method.invoke(s3ClientSupplier, args);
                }
            );

            // Create S3FileIO with the supplier
            return (T) s3FileIOClass.getConstructor(serializableSupplierClass).newInstance(serializableSupplier);

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                "Iceberg or AWS SDK classes not found on classpath. " + "Ensure iceberg-aws and AWS SDK dependencies are available.",
                e
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create S3FileIO via reflection", e);
        }
    }

    /**
     * Record of an S3 request for logging and analysis.
     */
    public static class S3RequestLog {
        private final String requestType;
        private final String path;
        private final long contentLength;
        private final long timestamp;

        public S3RequestLog(String requestType, String path, long contentLength, long timestamp) {
            this.requestType = requestType;
            this.path = path;
            this.contentLength = contentLength;
            this.timestamp = timestamp;
        }

        public String getRequestType() {
            return requestType;
        }

        public String getPath() {
            return path;
        }

        public long getContentLength() {
            return contentLength;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s (%d bytes)", requestType, path, contentLength);
        }
    }

    /**
     * Extended S3HttpFixture that automatically loads test fixtures from resources.
     * This fixture provides an in-memory S3-compatible endpoint for integration tests.
     */
    public static class DataSourcesS3HttpFixture extends S3HttpFixture {

        private static final Logger fixtureLogger = LogManager.getLogger(DataSourcesS3HttpFixture.class);

        private final int fixedPort;
        private S3HttpHandler handler;

        /**
         * Create a fixture with a random available port.
         */
        public DataSourcesS3HttpFixture() {
            this(-1);
        }

        /**
         * Create a fixture with a specific port.
         */
        public DataSourcesS3HttpFixture(int port) {
            super(true, () -> S3ConsistencyModel.STRONG_MPUS);
            this.fixedPort = port;
        }

        @Override
        protected S3HttpHandler createHandler() {
            BiPredicate<String, String> authPredicate = fixedAccessKey(ACCESS_KEY, () -> "us-east-1", "s3");
            handler = new LoggingS3HttpHandler(BUCKET, WAREHOUSE, S3ConsistencyModel.STRONG_MPUS, authPredicate);
            return handler;
        }

        /**
         * Get the underlying S3HttpHandler for direct blob manipulation.
         */
        public S3HttpHandler getHandler() {
            return handler;
        }

        /**
         * Load test fixtures from the classpath resources into the S3 fixture.
         */
        public void loadFixturesFromResources() {
            try {
                URL resourceUrl = getClass().getResource(FIXTURES_RESOURCE_PATH);
                if (resourceUrl == null) {
                    fixtureLogger.warn("Fixtures resource path not found: {}", FIXTURES_RESOURCE_PATH);
                    return;
                }

                if (resourceUrl.getProtocol().equals("file")) {
                    Path fixturesPath = Paths.get(resourceUrl.toURI());
                    loadFixturesFromPath(fixturesPath);
                } else {
                    fixtureLogger.warn("Cannot load fixtures from non-file URL: {}", resourceUrl);
                }
            } catch (Exception e) {
                fixtureLogger.error("Failed to load fixtures from resources", e);
            }
        }

        private void loadFixturesFromPath(Path fixturesPath) throws IOException {
            if (Files.exists(fixturesPath) == false) {
                fixtureLogger.warn("Fixtures path does not exist: {}", fixturesPath);
                return;
            }

            Set<String> loadedFiles = new HashSet<>();

            Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String relativePath = fixturesPath.relativize(file).toString();
                    String key = WAREHOUSE + "/" + relativePath;

                    byte[] content = Files.readAllBytes(file);
                    addBlobToFixture(handler, key, content);
                    loadedFiles.add(key);

                    return FileVisitResult.CONTINUE;
                }
            });

            fixtureLogger.info("Loaded {} fixture files from {}", loadedFiles.size(), fixturesPath);
        }

        /**
         * Load a single fixture file from an input stream.
         */
        public void loadFixture(String key, InputStream inputStream) throws IOException {
            byte[] content = inputStream.readAllBytes();
            addBlobToFixture(handler, key, content);
        }
    }

    /**
     * S3HttpHandler that logs all requests for analysis.
     */
    private static class LoggingS3HttpHandler extends S3HttpHandler {

        private final BiPredicate<String, String> authPredicate;

        LoggingS3HttpHandler(
            String bucket,
            String basePath,
            S3ConsistencyModel consistencyModel,
            BiPredicate<String, String> authPredicate
        ) {
            super(bucket, basePath, consistencyModel);
            this.authPredicate = authPredicate;
        }

        @Override
        public void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
            String method = exchange.getRequestMethod();
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();

            String requestType = classifyRequest(method, path, query);
            logRequest(requestType, path, 0);

            try {
                // Allow unauthenticated access when no Authorization header is present.
                // This enables plain HTTP clients (no S3 credentials) to read files from the fixture
                // while still verifying S3 auth when credentials are sent (e.g. from the AWS SDK).
                // NOTE: This means S3 auth bugs that cause missing Authorization headers will NOT
                // be caught by this fixture -- only requests that send incorrect credentials are rejected.
                String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
                if (authPredicate == null
                    || authHeader == null
                    || fixture.aws.AwsCredentialsUtils.checkAuthorization(authPredicate, exchange)) {
                    super.handle(exchange);
                }
            } catch (Exception e) {
                logger.error("Error handling S3 request: {} {}", method, path, e);
                throw e;
            }
        }

        private String classifyRequest(String method, String path, String query) {
            if ("GET".equals(method)) {
                if (query != null && query.contains("list-type=2")) {
                    return "LIST_OBJECTS_V2";
                } else if (query != null && query.contains("prefix=")) {
                    return "LIST_OBJECTS";
                } else if (query != null && query.contains("uploads")) {
                    return "LIST_MULTIPART_UPLOADS";
                }
                return "GET_OBJECT";
            } else if ("HEAD".equals(method)) {
                return "HEAD_OBJECT";
            } else if ("PUT".equals(method)) {
                if (query != null && query.contains("uploadId=") && query.contains("partNumber=")) {
                    return "UPLOAD_PART";
                }
                return "PUT_OBJECT";
            } else if ("DELETE".equals(method)) {
                if (query != null && query.contains("uploadId=")) {
                    return "ABORT_MULTIPART";
                }
                return "DELETE_OBJECT";
            } else if ("POST".equals(method)) {
                if (query != null && query.contains("uploads")) {
                    return "INITIATE_MULTIPART";
                } else if (query != null && query.contains("uploadId=")) {
                    return "COMPLETE_MULTIPART";
                } else if (query != null && query.contains("delete")) {
                    return "MULTI_OBJECT_DELETE";
                }
                return "UNKNOWN_POST";
            }
            return "UNKNOWN_" + method;
        }
    }
}
