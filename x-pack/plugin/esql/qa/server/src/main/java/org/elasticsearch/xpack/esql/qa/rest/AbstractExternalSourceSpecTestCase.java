/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import fixture.gcs.GoogleCloudStorageHttpFixture;
import fixture.gcs.TestUtils;

import com.github.luben.zstd.ZstdOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.S3RequestLog;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
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
        LOCAL,
        /** Google Cloud Storage via GoogleCloudStorageHttpFixture */
        GCS
    }

    private static final List<StorageBackend> BACKENDS = List.of(
        StorageBackend.S3,
        StorageBackend.HTTP,
        StorageBackend.LOCAL,
        StorageBackend.GCS
    );

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

    /**
     * Load csv-spec files and cross-product each test with all formats and storage backends.
     * Returns parameter arrays suitable for a {@code @ParametersFactory} constructor with 8 arguments:
     * (fileName, groupName, testName, lineNumber, testCase, instructions, format, storageBackend).
     */
    protected static List<Object[]> readExternalSpecTestsWithFormats(List<String> formats, String... specPatterns) throws Exception {
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
            for (String format : formats) {
                for (StorageBackend backend : BACKENDS) {
                    int baseLength = baseTest.length;
                    Object[] parameterizedTest = new Object[baseLength + 2];
                    System.arraycopy(baseTest, 0, parameterizedTest, 0, baseLength);
                    parameterizedTest[baseLength] = format;
                    parameterizedTest[baseLength + 1] = backend;
                    parameterizedTests.add(parameterizedTest);
                }
            }
        }
        return parameterizedTests;
    }

    @ClassRule
    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    /** GCS bucket name used by the GCS fixture */
    protected static final String GCS_BUCKET = "test-gcs-bucket";

    /** GCS OAuth2 token path used by the GCS fixture */
    protected static final String GCS_TOKEN = "o/oauth2/token";

    @ClassRule
    public static GoogleCloudStorageHttpFixture gcsFixture = new GoogleCloudStorageHttpFixture(true, GCS_BUCKET, GCS_TOKEN);

    /** Cached service account JSON for GCS authentication against the fixture */
    private static String gcsServiceAccountJson;

    /** Cached path to local fixtures directory */
    private static Path localFixturesPath;

    /** Compression suffixes to generate on the fly from .csv and .ndjson fixtures */
    private static final List<String> COMPRESSION_SUFFIXES = List.of(".gz", ".zst", ".zstd", ".bz2", ".bz");

    /**
     * Load fixtures from src/test/resources/iceberg-fixtures/ into the S3 and GCS fixtures.
     * Compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv and .ndjson files are generated
     * on the fly rather than checked in.
     */
    @BeforeClass
    public static void loadExternalSourceFixtures() {
        s3Fixture.loadFixturesFromResources();
        loadGcsFixtures();
        generateCompressedFixtures();
        resolveLocalFixturesPath();
    }

    /**
     * Generate a fake service account JSON and load fixture files into the GCS fixture.
     */
    private static void loadGcsFixtures() {
        try {
            byte[] serviceAccountBytes = TestUtils.createServiceAccount(random());
            gcsServiceAccountJson = new String(serviceAccountBytes, StandardCharsets.UTF_8);

            URL resourceUrl = AbstractExternalSourceSpecTestCase.class.getResource("/iceberg-fixtures");
            if (resourceUrl == null || "file".equals(resourceUrl.getProtocol()) == false) {
                logger.warn("Could not resolve iceberg-fixtures for GCS fixture loading");
                return;
            }

            Path fixturesPath = Paths.get(resourceUrl.toURI());
            if (Files.exists(fixturesPath) == false) {
                logger.warn("Fixtures path does not exist for GCS: {}", fixturesPath);
                return;
            }

            Set<String> loadedFiles = new HashSet<>();
            Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String name = file.getFileName().toString();
                    if (COMPRESSION_SUFFIXES.stream().anyMatch(name::endsWith)) {
                        return FileVisitResult.CONTINUE;
                    }
                    String relativePath = fixturesPath.relativize(file).toString();
                    String key = WAREHOUSE + "/" + relativePath;
                    byte[] content = Files.readAllBytes(file);
                    gcsFixture.getHandler().putBlob(key, new BytesArray(content));
                    loadedFiles.add(key);
                    return FileVisitResult.CONTINUE;
                }
            });

            logger.info("Loaded {} fixture files into GCS fixture", loadedFiles.size());
        } catch (Exception e) {
            logger.error("Failed to load GCS fixtures", e);
        }
    }

    /**
     * Generate compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv and .ndjson fixtures
     * on the fly and add them to the S3 and GCS fixtures. This avoids checking in binary
     * compressed files.
     */
    private static void generateCompressedFixtures() {
        try {
            URL resourceUrl = AbstractExternalSourceSpecTestCase.class.getResource("/iceberg-fixtures");
            if (resourceUrl == null || "file".equals(resourceUrl.getProtocol()) == false) {
                return;
            }
            Path fixturesPath = Paths.get(resourceUrl.toURI());
            if (Files.exists(fixturesPath) == false) {
                return;
            }

            int[] generated = { 0 };
            Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String name = file.getFileName().toString();
                    if (name.endsWith(".csv") == false && name.endsWith(".ndjson") == false) {
                        return FileVisitResult.CONTINUE;
                    }
                    byte[] content = Files.readAllBytes(file);
                    String relativeDir = fixturesPath.relativize(file.getParent()).toString();

                    for (String suffix : COMPRESSION_SUFFIXES) {
                        byte[] compressed = compress(content, suffix);
                        String compressedName = name + suffix;
                        String key = WAREHOUSE + "/" + (relativeDir.isEmpty() ? compressedName : relativeDir + "/" + compressedName);

                        addBlobToFixture(key, compressed);
                        gcsFixture.getHandler().putBlob(key, new BytesArray(compressed));
                        generated[0]++;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
            if (generated[0] > 0) {
                logger.info("Generated {} compressed fixture variants on the fly", generated[0]);
            }
        } catch (Exception e) {
            logger.error("Failed to generate compressed fixtures", e);
        }
    }

    private static byte[] compress(byte[] input, String suffix) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return switch (suffix) {
            case ".gz" -> {
                try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            case ".zst", ".zstd" -> {
                try (ZstdOutputStream out = new ZstdOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            case ".bz2", ".bz" -> {
                try (BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(baos)) {
                    out.write(input);
                }
                yield baos.toByteArray();
            }
            default -> throw new IllegalArgumentException("Unknown compression: " + suffix);
        };
    }

    /**
     * Resolve and cache the local path to the fixtures directory.
     * Writes generated compressed variants (.gz, .zst, .zstd, .bz2, .bz) alongside the
     * source fixtures so the LOCAL storage backend can access them from the same path.
     */
    private static void resolveLocalFixturesPath() {
        try {
            URL resourceUrl = AbstractExternalSourceSpecTestCase.class.getResource("/iceberg-fixtures");
            if (resourceUrl == null || "file".equals(resourceUrl.getProtocol()) == false) {
                logger.warn("Could not resolve local fixtures path - LOCAL storage backend may not work");
                return;
            }
            Path fixturesPath = Paths.get(resourceUrl.toURI());
            if (Files.exists(fixturesPath) == false) {
                return;
            }
            writeCompressedVariantsToFixturesPath(fixturesPath);
            localFixturesPath = fixturesPath;
            logger.info("Local fixtures path: {}", localFixturesPath);
        } catch (Exception e) {
            logger.warn("Failed to resolve local fixtures path", e);
        }
    }

    private static void writeCompressedVariantsToFixturesPath(Path fixturesPath) throws IOException {
        Files.walkFileTree(fixturesPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String name = file.getFileName().toString();
                if (name.endsWith(".csv") || name.endsWith(".ndjson")) {
                    byte[] content = Files.readAllBytes(file);
                    Path parent = file.getParent();
                    for (String suffix : COMPRESSION_SUFFIXES) {
                        byte[] compressed = compress(content, suffix);
                        Files.write(parent.resolve(name + suffix), compressed);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
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
        logger.info("GCS Fixture endpoint: {}", gcsFixture.getAddress());
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
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
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

        // Inject endpoint and credentials for GCS backend
        if (storageBackend == StorageBackend.GCS && isExternalQuery(query) && hasEndpointParam(query) == false) {
            query = injectGcsParams(query);
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
                    return localFile.toUri().toString();
                } else {
                    // Fallback to S3 if local path not available
                    logger.warn("Local fixtures path not available, falling back to S3");
                    return "s3://" + BUCKET + "/" + WAREHOUSE + "/" + relativePath;
                }

            case GCS:
                // GCS path: gs://bucket/warehouse/standalone/employees.parquet
                return "gs://" + GCS_BUCKET + "/" + WAREHOUSE + "/" + relativePath;

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
     * Inject GCS endpoint, credentials, and project_id into the query.
     */
    private String injectGcsParams(String query) {
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

        // Escape the service account JSON for embedding inside the WITH clause.
        // The JSON is embedded as a string value, so internal double-quotes must be escaped.
        String escapedCredentials = gcsServiceAccountJson.replace("\\", "\\\\").replace("\"", "\\\"");

        String tokenUri = gcsFixture.getAddress() + "/" + GCS_TOKEN;

        StringBuilder params = new StringBuilder();
        params.append(" WITH { ");
        params.append("\"endpoint\": \"").append(gcsFixture.getAddress()).append("\", ");
        params.append("\"credentials\": \"").append(escapedCredentials).append("\", ");
        params.append("\"project_id\": \"test\", ");
        params.append("\"token_uri\": \"").append(tokenUri).append("\"");
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

    protected static String getGcsEndpoint() {
        return gcsFixture.getAddress();
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
