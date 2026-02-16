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
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.S3RequestLog;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.ACCESS_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.SECRET_KEY;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Abstract base class for external source integration tests using S3HttpFixture.
 * Provides common S3 fixture infrastructure for testing external data sources like Iceberg and Parquet.
 * <p>
 * This class provides template-based query transformation where templates like {@code {{employees}}}
 * are replaced with actual paths based on the storage backend (S3, HTTP, LOCAL) and format (parquet, csv).
 * <p>
 * Subclasses specify the storage backend and format in their constructor, and the base class handles
 * all path resolution automatically.
 *
 * @see S3FixtureUtils for shared S3 fixture utilities
 */
public abstract class AbstractExternalSourceSpecTestCase extends EsqlSpecTestCase {

    private static final Logger logger = LogManager.getLogger(AbstractExternalSourceSpecTestCase.class);

    /** Pattern to match template placeholders like {{employees}} */
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{(\\w+)}}");

    /** Base path for fixtures within the resource directory */
    private static final String FIXTURES_BASE = "standalone";

    /**
     * Storage backend for accessing external files.
     */
    public enum StorageBackend {
        /** S3 storage via S3HttpFixture */
        S3,
        /** HTTP storage via S3HttpFixture (same endpoint, different protocol) */
        HTTP,
        /** Local file system storage (direct classpath resource access) */
        LOCAL
    }

    private static final List<StorageBackend> BACKENDS = List.of(StorageBackend.S3, StorageBackend.HTTP, StorageBackend.LOCAL);

    /**
     * Load csv-spec files matching the given patterns and cross-product each test with all storage backends.
     * Returns parameter arrays suitable for a {@code @ParametersFactory} constructor with 7 arguments:
     * (fileName, groupName, testName, lineNumber, testCase, instructions, storageBackend).
     */
    protected static List<Object[]> readExternalSpecTests(String... specPatterns) throws Exception {
        List<URL> urls = new ArrayList<>();
        for (String pattern : specPatterns) {
            urls.addAll(classpathResources(pattern));
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("No csv-spec files found for patterns: " + List.of(specPatterns));
        }

        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, specParser());
        List<Object[]> parameterizedTests = new ArrayList<>();
        for (Object[] baseTest : baseTests) {
            for (StorageBackend backend : BACKENDS) {
                int baseLength = baseTest.length;
                Object[] parameterizedTest = new Object[baseLength + 1];
                System.arraycopy(baseTest, 0, parameterizedTest, 0, baseLength);
                parameterizedTest[baseLength] = backend;
                parameterizedTests.add(parameterizedTest);
            }
        }
        return parameterizedTests;
    }

    @ClassRule
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    /** Cached path to local fixtures directory */
    private static Path localFixturesPath;

    /**
     * Load fixtures from src/test/resources/iceberg-fixtures/ into the S3 fixture.
     * This runs once before all tests, making pre-built test data available automatically.
     */
    @BeforeClass
    public static void loadExternalSourceFixtures() {
        s3Fixture.loadFixturesFromResources();
        resolveLocalFixturesPath();
    }

    /**
     * Resolve and cache the local path to the fixtures directory.
     * This is used for LOCAL storage backend to access files directly from the classpath.
     */
    private static void resolveLocalFixturesPath() {
        try {
            URL resourceUrl = AbstractExternalSourceSpecTestCase.class.getResource("/iceberg-fixtures");
            if (resourceUrl != null && resourceUrl.getProtocol().equals("file")) {
                localFixturesPath = Paths.get(resourceUrl.toURI());
                logger.info("Local fixtures path: {}", localFixturesPath);
            } else {
                logger.warn("Could not resolve local fixtures path - LOCAL storage backend may not work");
            }
        } catch (URISyntaxException e) {
            logger.warn("Failed to resolve local fixtures path", e);
        }
    }

    /**
     * Skip standard test data loading for external source tests.
     */
    @BeforeClass
    public static void skipStandardDataLoading() {
        try {
            java.lang.reflect.Field ingestField = EsqlSpecTestCase.class.getDeclaredField("INGEST");
            ingestField.setAccessible(true);
            Object ingest = ingestField.get(null);

            java.lang.reflect.Field completedField = ingest.getClass().getDeclaredField("completed");
            completedField.setAccessible(true);
            completedField.setBoolean(ingest, true);

            logger.info("Skipped standard test data loading for external source tests");
        } catch (Exception e) {
            logger.warn("Failed to skip standard data loading, tests may be slower", e);
        }
    }

    @BeforeClass
    public static void verifySetup() {
        logger.info("=== External Source Test Setup Verification ===");
        logger.info("S3 Fixture endpoint: {}", s3Fixture.getAddress());
        logger.info("Local fixtures path: {}", localFixturesPath);
    }

    /**
     * Automatically checks for unsupported S3 operations after each test.
     */
    @org.junit.After
    public void checkForUnsupportedOperations() {
        String errorMessage = S3FixtureUtils.buildUnsupportedOperationsError();
        if (errorMessage != null) {
            fail(errorMessage);
        }
    }

    private final StorageBackend storageBackend;
    private final String format;

    protected AbstractExternalSourceSpecTestCase(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions,
        StorageBackend storageBackend,
        String format
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
        this.storageBackend = storageBackend;
        this.format = format;
    }

    /**
     * Get the storage backend for this test.
     */
    protected StorageBackend getStorageBackend() {
        return storageBackend;
    }

    /**
     * Get the format (e.g., "parquet", "csv") for this test.
     */
    protected String getFormat() {
        return format;
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // skip nothing
        // super skips tests for the "regular" CsvTest/EsqlSpecIT suites
    }

    /**
     * Override doTest() to transform templates and inject storage-specific parameters.
     */
    @Override
    protected void doTest() throws Throwable {
        String query = testCase.query;

        if (query.contains(MULTIFILE_SUFFIX)) {
            // HTTP does not support directory listing, so skip multi-file glob tests
            assumeTrue("HTTP backend does not support multi-file glob patterns", storageBackend != StorageBackend.HTTP);
            // CSV format does not yet support multi-file glob patterns
            assumeTrue("CSV format does not support multi-file glob patterns", "csv".equals(format) == false);

        }

        // Transform templates like {{employees}} to actual paths
        query = transformTemplates(query);

        // Inject endpoint and credentials for S3 backend
        if (storageBackend == StorageBackend.S3 && isExternalQuery(query) && hasEndpointParam(query) == false) {
            query = injectS3Params(query);
        }

        logger.debug("Transformed query for {} backend: {}", storageBackend, query);
        doTest(query);
    }

    /**
     * Transform template placeholders in the query.
     * Replaces {{anything}} with the actual path based on storage backend and format.
     *
     * @param query the query with template placeholders
     * @return the query with templates replaced by actual paths
     */
    private String transformTemplates(String query) {
        Matcher matcher = TEMPLATE_PATTERN.matcher(query);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String templateName = matcher.group(1);
            String resolvedPath = resolveTemplatePath(templateName);
            matcher.appendReplacement(result, Matcher.quoteReplacement(resolvedPath));
        }
        matcher.appendTail(result);

        return result.toString();
    }

    /** Suffix that triggers multi-file glob resolution */
    private static final String MULTIFILE_SUFFIX = "_multifile";

    /**
     * Resolve a template name to an actual path based on storage backend and format.
     *
     * @param templateName the template name (e.g., "employees" or "employees_multifile")
     * @return the resolved path
     */
    private String resolveTemplatePath(String templateName) {
        String relativePath;
        if (templateName.endsWith(MULTIFILE_SUFFIX)) {
            // Multi-file template: employees_multifile -> multifile/*.parquet
            relativePath = "multifile/*." + format;
        } else {
            // Single-file template: employees -> standalone/employees.parquet
            String filename = templateName + "." + format;
            relativePath = FIXTURES_BASE + "/" + filename;
        }

        switch (storageBackend) {
            case S3:
                // S3 path: s3://bucket/warehouse/standalone/employees.parquet
                return "s3://" + BUCKET + "/" + WAREHOUSE + "/" + relativePath;

            case HTTP:
                // HTTP path: http://host:port/bucket/warehouse/standalone/employees.parquet
                return s3Fixture.getAddress() + "/" + BUCKET + "/" + WAREHOUSE + "/" + relativePath;

            case LOCAL:
                // Local path: file:///absolute/path/to/iceberg-fixtures/standalone/employees.parquet
                if (localFixturesPath != null) {
                    Path localFile = localFixturesPath.resolve(relativePath);
                    return "file://" + localFile.toAbsolutePath().toString();
                } else {
                    // Fallback to S3 if local path not available
                    logger.warn("Local fixtures path not available, falling back to S3");
                    return "s3://" + BUCKET + "/" + WAREHOUSE + "/" + relativePath;
                }

            default:
                throw new IllegalArgumentException("Unknown storage backend: " + storageBackend);
        }
    }

    /**
     * Inject S3 endpoint and credentials into the query.
     */
    private String injectS3Params(String query) {
        String trimmed = query.trim();
        int pipeIndex = findFirstPipeAfterExternal(trimmed);

        String externalPart;
        String restOfQuery;

        if (pipeIndex == -1) {
            externalPart = trimmed;
            restOfQuery = "";
        } else {
            externalPart = trimmed.substring(0, pipeIndex).trim();
            restOfQuery = " " + trimmed.substring(pipeIndex);
        }

        StringBuilder params = new StringBuilder();
        params.append(" WITH { ");
        params.append("\"endpoint\": \"").append(s3Fixture.getAddress()).append("\", ");
        params.append("\"access_key\": \"").append(ACCESS_KEY).append("\", ");
        params.append("\"secret_key\": \"").append(SECRET_KEY).append("\"");
        params.append(" }");

        return externalPart + params.toString() + restOfQuery;
    }

    /**
     * Check if query starts with EXTERNAL command.
     */
    private static boolean isExternalQuery(String query) {
        return query.trim().toUpperCase(Locale.ROOT).startsWith("EXTERNAL");
    }

    /**
     * Check if query already has endpoint parameter.
     */
    private static boolean hasEndpointParam(String query) {
        return query.toLowerCase(Locale.ROOT).contains("endpoint");
    }

    /**
     * Find the first pipe character that's not inside a quoted string.
     */
    private static int findFirstPipeAfterExternal(String query) {
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);

            if (inQuotes == false && (c == '"' || c == '\'')) {
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
            } else if (inQuotes == false && c == '|') {
                return i;
            }
        }

        return -1;
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    // Static utility methods for fixture access

    protected static String getS3Endpoint() {
        return s3Fixture.getAddress();
    }

    protected static List<S3RequestLog> getRequestLogs() {
        return S3FixtureUtils.getRequestLogs();
    }

    protected static void clearRequestLogs() {
        S3FixtureUtils.clearRequestLogs();
    }

    protected static void printRequestSummary() {
        S3FixtureUtils.printRequestSummary();
    }

    protected static int getRequestCount(String requestType) {
        return S3FixtureUtils.getRequestCount(requestType);
    }

    protected static List<S3RequestLog> getRequestsByType(String requestType) {
        return S3FixtureUtils.getRequestsByType(requestType);
    }

    protected static boolean hasUnknownRequests() {
        return S3FixtureUtils.hasUnknownRequests();
    }

    protected static List<S3RequestLog> getUnknownRequests() {
        return S3FixtureUtils.getUnknownRequests();
    }

    protected static void addBlobToFixture(String key, String content) {
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), key, content);
    }

    protected static void addBlobToFixture(String key, byte[] content) {
        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), key, content);
    }

    protected static String getWarehousePath() {
        return S3FixtureUtils.getWarehousePath();
    }
}
