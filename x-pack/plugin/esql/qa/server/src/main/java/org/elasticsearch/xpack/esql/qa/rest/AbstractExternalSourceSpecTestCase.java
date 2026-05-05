/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.rest;

import org.elasticsearch.Version;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.DataSourcesGcsHttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.S3RequestLog;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.ACCOUNT;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.CONTAINER;
import static org.elasticsearch.xpack.esql.datasources.FixtureUtils.COMPRESSED_EXTENSIONS;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
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
        GCS,
        /** Azure Blob Storage via AzureHttpFixture */
        AZURE
    }

    private static final List<StorageBackend> BACKENDS;

    static {
        List<StorageBackend> backends = new ArrayList<>(
            List.of(StorageBackend.S3, StorageBackend.HTTP, StorageBackend.GCS, StorageBackend.AZURE)
        );
        if (FixtureUtils.resolveLocalFixturesPath(logger, AbstractExternalSourceSpecTestCase.class) != null) {
            backends.add(StorageBackend.LOCAL);
        }
        BACKENDS = List.copyOf(backends);
    }

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

    @ClassRule
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture();

    @ClassRule
    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();

    /** Cached path to local fixtures directory */
    private static Path localFixturesPath;

    /**
     * Load fixtures from src/test/resources/iceberg-fixtures/ into the S3 and GCS fixtures.
     * Compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv and .ndjson files are generated
     * on the fly rather than checked in.
     */
    @BeforeClass
    public static void loadExternalSourceFixtures() {
        s3Fixture.loadFixturesFromResources();
        gcsFixture.loadFixturesFromResources();
        azureFixture.loadFixturesFromResources();
        generateCompressedFixtures();
        resolveLocalFixturesPath();
    }

    /**
     * Generate compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv and .ndjson fixtures
     * on the fly and add them to the S3, GCS, and Azure fixtures. This avoids checking in binary
     * compressed files.
     */
    private static void generateCompressedFixtures() {
        try {
            int[] generated = { 0 };
            FixtureUtils.forEachFixtureEntryMergingAllClasspathRoots(
                AbstractExternalSourceSpecTestCase.class.getClassLoader(),
                (relativePath, content) -> {
                    String fileName = relativePath.contains("/") ? relativePath.substring(relativePath.lastIndexOf('/') + 1) : relativePath;
                    if (fileName.endsWith(".csv") == false && fileName.endsWith(".ndjson") == false) {
                        return;
                    }
                    String relativeDir = relativePath.contains("/") ? relativePath.substring(0, relativePath.lastIndexOf('/')) : "";

                    for (String suffix : COMPRESSED_EXTENSIONS) {
                        byte[] compressed = FixtureUtils.compress(content, suffix);
                        String compressedName = fileName + suffix;
                        String key = WAREHOUSE + "/" + (relativeDir.isEmpty() ? compressedName : relativeDir + "/" + compressedName);

                        S3FixtureUtils.addBlobToFixture(s3Fixture.getHandler(), key, compressed);
                        GcsFixtureUtils.addBlobToFixture(gcsFixture.getHandler(), key, compressed);
                        AzureFixtureUtils.addBlobToFixture(azureFixture.getAddress(), key, compressed);
                        generated[0]++;
                    }
                }
            );
            logger.info("Generated {} compressed fixture variants", generated[0]);
        } catch (Exception e) {
            logger.error("Failed to generate compressed fixtures", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Resolve and cache the local path to the fixtures directory.
     * Writes generated compressed variants (.gz, .zst, .zstd, .bz2, .bz) alongside the
     * source fixtures so the LOCAL storage backend can access them from the same path.
     * When fixtures are packaged in a JAR, the local path is unavailable and LOCAL backend
     * tests will be skipped.
     */
    private static void resolveLocalFixturesPath() {
        Path fixturesPath = FixtureUtils.resolveLocalFixturesPath(logger, AbstractExternalSourceSpecTestCase.class);
        if (fixturesPath != null) {
            try {
                FixtureUtils.writeCompressedVariantsToFixturesPath(fixturesPath);
                localFixturesPath = fixturesPath;
                logger.info("Local fixtures path: {}", localFixturesPath);
            } catch (Exception e) {
                logger.warn("Failed to resolve local fixtures path", e);
                throw new RuntimeException(e);
            }
        } else {
            logger.info("Fixtures are inside a JAR; LOCAL storage backend will not be available");
            localFixturesPath = null;
        }
    }

    @BeforeClass
    public static void logSetup() {
        logger.info("=== External Source Test Setup Verification ===");
        logger.info("S3 Fixture endpoint: {}", s3Fixture.getAddress());
        logger.info("GCS Fixture endpoint: {}", gcsFixture.getAddress());
        logger.info("Azure Fixture endpoint: {}", azureFixture.getAddress());
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
    /**
     * Per-test choice of Azure URI form, set once in {@link #doTest()} so that all template
     * substitutions within a single test (including wildcard expansions returning multiple files)
     * see a consistent form. Both forms are equivalent; randomising per test exercises both.
     */
    private boolean useAzureHadoopForm;

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

        // Pick the Azure URI form once per test so wildcard expansion sees a single, consistent form.
        useAzureHadoopForm = storageBackend == StorageBackend.AZURE && randomBoolean();

        // Transform templates like {{employees}} to actual paths
        query = transformTemplates(query);

        // Inject endpoint and credentials for S3 backend
        if (isExternalQuery(query)) {
            query = switch (storageBackend) {
                case StorageBackend.S3 -> s3Fixture.injectParams(query);
                case StorageBackend.GCS -> gcsFixture.injectParams(query);
                case StorageBackend.AZURE -> azureFixture.injectParams(query);
                default -> query;
            };
        }

        logger.debug("Transformed query for {} backend: {}", storageBackend, query);
        doTest(query);
    }

    /**
     * Check if query starts with EXTERNAL command.
     */
    private static boolean isExternalQuery(String query) {
        return query.trim().toUpperCase(Locale.ROOT).startsWith("EXTERNAL");
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
        StringBuilder result = new StringBuilder();

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
                return "gs://" + GcsFixtureUtils.BUCKET + "/" + WAREHOUSE + "/" + relativePath;

            case AZURE:
                // Azure has two equivalent URI forms; the choice is made once per test in doTest().
                // Path-style: wasbs://account.blob.core.windows.net/container/warehouse/.../employees.parquet
                // Hadoop: wasbs://container@account.blob.core.windows.net/warehouse/.../employees.parquet
                if (useAzureHadoopForm) {
                    return "wasbs://" + CONTAINER + "@" + ACCOUNT + ".blob.core.windows.net/" + WAREHOUSE + "/" + relativePath;
                }
                return "wasbs://" + ACCOUNT + ".blob.core.windows.net/" + CONTAINER + "/" + WAREHOUSE + "/" + relativePath;

            default:
                throw new IllegalArgumentException("Unknown storage backend: " + storageBackend);
        }
    }

    @Override
    protected List<String> indicesToLoad() {
        // languages: enrich policy source; languages_lookup: LOOKUP JOIN (see CsvTestsDataLoader.loadEnrichPoliciesForLoadedSourceIndices)
        return List.of("languages", "languages_lookup");
    }

    @Override
    protected boolean supportsInferenceTestServiceOnLocalCluster() {
        return false;
    }

    @Override
    protected void createInferenceEndpointsIfSupported() throws IOException {
        // Register only RERANK: external-basic.csv-spec uses test_reranker; full INFERENCE_CONFIGS includes task types
        // not supported on these minimal clusters (e.g. SPARSE_EMBEDDING). Test clusters must load inference-service-test.
        CsvTestsDataLoader.createInferenceEndpoints(adminClient(), List.of("test_reranker"));
    }

    @Override
    protected boolean supportsSemanticTextInference() {
        return false;
    }

    // Static utility methods for fixture access

    protected static List<S3RequestLog> getRequestLogs() {
        return S3FixtureUtils.getRequestLogs();
    }
}
