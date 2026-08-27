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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.CsvSpecReader.DatasetSource;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.SpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.DatasetRegistry;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;
import org.elasticsearch.xpack.esql.datasources.FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.DataSourcesGcsHttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.S3RequestLog;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureExclusions;
import org.elasticsearch.xpack.esql.datasources.fixtures.FixtureMatrix;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.ACCOUNT;
import static org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.CONTAINER;
import static org.elasticsearch.xpack.esql.datasources.FixtureUtils.COMPRESSED_EXTENSIONS;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

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
    /**
     * The one {@code {{template}}} grammar. There were two regexes for this, disagreeing on case --
     * {@code \\w+} accepted uppercase and {@code [a-z0-9_]+} did not -- so a template name would
     * eventually have resolved on one path and silently not on the other.
     */
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{(\\w+)}}");

    /** Default base path for fixtures within the resource directory */
    private static final String FIXTURES_BASE = FixtureMatrix.get().layout(FixtureMatrix.STANDALONE).dir();

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
    /**
     * Loads the csv-spec files declared for a suite in {@code suite.<token>.specs}, rather than a list
     * written out here.
     *
     * <p>The declaration has two consumers -- this method and the coverage gate that asks whether a
     * declared fixture cell has a reader. While the list lived in the suite, the gate could only
     * approximate it by scanning directories, and a spec in a scanned directory that no suite loaded still
     * counted as a consumer: that reported the csv column covered for hive_shadow while every one of the
     * ten shadow executions was TSV.
     */
    protected static List<Object[]> readExternalSpecTestsForSuite(String suiteToken) throws Exception {
        Set<String> excluded = MATRIX.excludedSpecs(suiteToken);
        List<Object[]> loaded = readExternalSpecTests(MATRIX.specPatterns(suiteToken).toArray(String[]::new));
        if (excluded.isEmpty()) {
            return loaded;
        }
        // Drop whole spec files the declaration excludes for this suite. Filtering here rather than
        // assuming away per case at run time: a case that is registered and then always skipped is a skip
        // no gate can see and no report can count.
        return loaded.stream().filter(row -> excluded.contains(specNameOf(row)) == false).toList();
    }

    /** The spec file name (without extension) a parameterised row came from -- element 0 is the file name. */
    private static String specNameOf(Object[] row) {
        String fileName = String.valueOf(row[0]);
        return fileName.endsWith(".csv-spec") ? fileName.substring(0, fileName.length() - ".csv-spec".length()) : fileName;
    }

    protected static List<Object[]> readExternalSpecTests(String... specPatterns) throws Exception {
        List<URL> urls = new ArrayList<>();
        for (String pattern : specPatterns) {
            List<URL> matched = classpathResources(pattern);
            // Per pattern, not just in aggregate. A literal route to a spec that no longer exists
            // matches nothing and contributes nothing, and the aggregate check cannot see it while any
            // sibling pattern still resolves -- which is how suite.csv-compressed kept routing a spec
            // this branch had already deleted, losing 13 cases with every gate green.
            if (matched.isEmpty()) {
                throw new IllegalStateException(
                    "spec pattern ["
                        + pattern
                        + "] matches no file on this suite's classpath; "
                        + "the routing in fixture-matrix.properties names a spec that does not exist"
                );
            }
            urls.addAll(matched);
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("No csv-spec files found for patterns: " + List.of(specPatterns));
        }

        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
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
    /**
     * Codec-fanned variant of {@link #readExternalSpecTestsForSuite}: the spec list comes from
     * {@code suite.<token>.specs} rather than from a list written at the call site.
     *
     * <p>Without this the compressed suites could not read the declaration at all, so the declaration and
     * the suite drifted -- the declaration claimed twelve specs for ndjson-compressed while the suite
     * hard-coded six, and the coverage gate believed the declaration. That is the same false green the gate
     * exists to prevent, reintroduced one level up.
     */
    protected static List<Object[]> readExternalSpecTestsWithFormatsForSuite(List<String> formats, String suiteToken) throws Exception {
        Set<String> excluded = MATRIX.excludedSpecs(suiteToken);
        List<Object[]> loaded = readExternalSpecTestsWithExtraParam(formats, MATRIX.specPatterns(suiteToken).toArray(String[]::new));
        return excluded.isEmpty() ? loaded : loaded.stream().filter(row -> excluded.contains(specNameOf(row)) == false).toList();
    }

    protected static List<Object[]> readExternalSpecTestsWithFormats(List<String> formats, String... specPatterns) throws Exception {
        return readExternalSpecTestsWithExtraParam(formats, specPatterns);
    }

    /**
     * Load csv-spec files and cross-product each test with all codecs and storage backends.
     * Returns parameter arrays suitable for a {@code @ParametersFactory} constructor with 8 arguments:
     * (fileName, groupName, testName, lineNumber, testCase, instructions, codecName, storageBackend).
     * Identical shape to {@link #readExternalSpecTestsWithFormats}; the separate name documents the
     * intent of the extra column ("codec" vs. "format") at the call site.
     */
    protected static List<Object[]> readExternalSpecTestsWithCodecs(List<String> codecs, String... specPatterns) throws Exception {
        return readExternalSpecTestsWithExtraParam(codecs, specPatterns);
    }

    /**
     * Codec cross-product with the spec set read from the declaration rather than listed at the call site.
     * The codec counterpart of {@link #readExternalSpecTestsWithFormatsForSuite}; see that method for why
     * whole-spec exclusions are filtered here instead of being skipped per case at run time.
     */
    protected static List<Object[]> readExternalSpecTestsWithCodecsForSuite(List<String> codecs, String suiteToken) throws Exception {
        Set<String> excluded = MATRIX.excludedSpecs(suiteToken);
        List<Object[]> loaded = readExternalSpecTestsWithExtraParam(codecs, MATRIX.specPatterns(suiteToken).toArray(String[]::new));
        return excluded.isEmpty() ? loaded : loaded.stream().filter(row -> excluded.contains(specNameOf(row)) == false).toList();
    }

    /**
     * Shared cross-product helper used by {@link #readExternalSpecTestsWithFormats} and
     * {@link #readExternalSpecTestsWithCodecs}. Builds the cross product on the un-expanded base tuple
     * (so the resulting array is always {@code (baseTest..., extraParam, backend)}) rather than splicing
     * into a tuple that already has the backend appended.
     */
    private static List<Object[]> readExternalSpecTestsWithExtraParam(List<String> extraParams, String... specPatterns) throws Exception {
        List<URL> urls = new ArrayList<>();
        for (String pattern : specPatterns) {
            List<URL> matched = classpathResources(pattern);
            // Per pattern, not just in aggregate. A literal route to a spec that no longer exists
            // matches nothing and contributes nothing, and the aggregate check cannot see it while any
            // sibling pattern still resolves -- which is how suite.csv-compressed kept routing a spec
            // this branch had already deleted, losing 13 cases with every gate green.
            if (matched.isEmpty()) {
                throw new IllegalStateException(
                    "spec pattern ["
                        + pattern
                        + "] matches no file on this suite's classpath; "
                        + "the routing in fixture-matrix.properties names a spec that does not exist"
                );
            }
            urls.addAll(matched);
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException("No csv-spec files found for patterns: " + List.of(specPatterns));
        }

        List<Object[]> baseTests = SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
        List<Object[]> parameterizedTests = new ArrayList<>();
        for (Object[] baseTest : baseTests) {
            for (String extra : extraParams) {
                for (StorageBackend backend : BACKENDS) {
                    int baseLength = baseTest.length;
                    Object[] parameterizedTest = new Object[baseLength + 2];
                    System.arraycopy(baseTest, 0, parameterizedTest, 0, baseLength);
                    parameterizedTest[baseLength] = extra;
                    parameterizedTest[baseLength + 1] = backend;
                    parameterizedTests.add(parameterizedTest);
                }
            }
        }
        return parameterizedTests;
    }

    public static DataSourcesS3HttpFixture s3Fixture = new DataSourcesS3HttpFixture();

    // Anonymous form: migrated specs read every backend via FROM <dataset> with auth=anonymous, so the
    // Azure fixture must serve unauthenticated reads (the S3/GCS fixtures already do). No shared-key
    // secret is stored, so these suites need no cluster encryption key.
    public static DataSourcesAzureHttpFixture azureFixture = new DataSourcesAzureHttpFixture(true);

    public static DataSourcesGcsHttpFixture gcsFixture = new DataSourcesGcsHttpFixture();

    /**
     * Builds a {@link ClassRule} that starts object-store fixtures before the test cluster. Without an
     * explicit order, JUnit may boot the cluster while fixtures are not yet listening and external reads
     * fail with transient {@code Connection is closed} errors (especially on Azure).
     */
    protected static TestRule chainFixturesBeforeCluster(ElasticsearchCluster cluster) {
        return RuleChain.outerRule(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);
    }

    /**
     * Like {@link #chainFixturesBeforeCluster(ElasticsearchCluster)} but runs {@code outer} first (e.g.
     * an {@code assumeFalse} guard) before bringing up fixtures and the cluster.
     */
    protected static TestRule chainOuterRuleBeforeFixturesAndCluster(TestRule outer, ElasticsearchCluster cluster) {
        return RuleChain.outerRule(outer).around(s3Fixture).around(gcsFixture).around(azureFixture).around(cluster);
    }

    /** Cached path to local fixtures directory */
    private static Path localFixturesPath;

    /**
     * Load fixtures from src/test/resources/iceberg-fixtures/ into the S3, GCS, and Azure fixtures.
     * Compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv, .ndjson, and .tsv files are generated
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
     * Generate compressed variants (.gz, .zst, .zstd, .bz2, .bz) of .csv, .ndjson, and .tsv fixtures
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
                    if (fileName.endsWith(".csv") == false && fileName.endsWith(".ndjson") == false && fileName.endsWith(".tsv") == false) {
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
     * Drops every {@code data_source}/{@code dataset} registered by {@link DatasetRegistry} during the
     * suite (datasets first, so data-source deletes do not 409 on a still-referenced parent). These are
     * {@code ProjectCustom} metadata that survive the framework's index wipe, so they must be cleaned
     * explicitly. The cluster-side delete is skipped when the test clusters are already known broken, but
     * the static caches are always cleared (in a {@code finally}) so a broken cluster — or a cleanup that
     * throws partway — cannot poison a later suite sharing this JVM fork.
     */
    @AfterClass
    public static void cleanupRegisteredDatasets() throws IOException {
        try {
            if (testClustersOk) {
                DatasetRegistry.cleanup(adminClient());
            }
        } finally {
            DatasetRegistry.clearCaches();
            declaredSchemaSupported = null;
        }
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
    private final String specName;
    /**
     * Per-test choice of Azure URI form, set once in {@link #doTest()} so that all template
     * substitutions within a single test (including wildcard expansions returning multiple files)
     * see a consistent form. Both forms are equivalent; randomising per test exercises both.
     */
    private boolean useAzureHadoopForm;

    /**
     * Per-test memos for the two questions that would otherwise re-parse every directive's {@code WITH} JSON on
     * each ask. {@code ensureDataset} deliberately keys its cache off the RAW text so a registration parses only
     * on a cache miss; the declared-schema guards and the trim_spaces injector would have undone that by parsing
     * per call. Both answers are fixed for a test instance -- the directives do not change mid-test.
     */
    private Boolean declaresMappingsMemo;
    private final Map<DatasetSource, String> withJsonMemo = new IdentityHashMap<>();

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
        this.specName = groupName;
    }

    /**
     * The token this suite's exclusions are declared under in {@code fixture-exclusions.properties}.
     *
     * <p>Defaults to the base format with any codec stripped, which is correct for the suites whose
     * declaration token IS their format ({@code csv}, {@code tsv}, {@code ndjson}, {@code orc},
     * {@code parquet}). A suite whose token diverges from its format MUST override this: the
     * parquet-rs suite runs with format {@code "parquet"} (its reader is selected separately) and the
     * compressed suites carry a codec, so for those the default would silently resolve to another
     * suite's exclusion set.
     */
    protected String exclusionSuiteToken() {
        return FixtureMatrix.baseFormat(format);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        // One skip path for every external-source suite, reading the single declaration. The message is the
        // declared reason, so a skip explains itself in the log rather than asserting a hard-coded cause.
        FixtureExclusions.Exclusion exclusion = FixtureExclusions.get().find(exclusionSuiteToken(), specName, testName);
        assumeTrue(exclusion == null ? "" : testName + ": " + exclusion.reason(), exclusion == null);
        checkCapabilities(adminClient(), testFeatureService, testName, testCase);
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
    }

    /**
     * Override doTest() to transform templates and inject storage-specific parameters.
     * <p>
     * A spec that declares {@code dataset:} sources runs via the native {@code FROM <dataset>} path on
     * every storage backend: the datasets are registered and the spec's {@code FROM <name>} query is run
     * verbatim (see {@link #runDatasetMode()}). There is no longer an {@code EXTERNAL}-rebuild fallback.
     * <p>
     * Specs with no {@code dataset:} directive are run as-is. This still covers raw {@code EXTERNAL}
     * queries that cannot be expressed as a dataset because their backend registers no
     * {@code DataSourceValidator} — today the Iceberg suite ({@code IcebergSpecTestCase}), which reaches
     * its table via {@code EXTERNAL "s3://..." WITH { "format": "iceberg" }}.
     */
    @Override
    protected void doTest() throws Throwable {
        if (testCase.datasetSources.isEmpty() == false && forceExternalRebuild() == false) {
            runDatasetMode();
            return;
        }

        // A multi-source FROM <dataset> has no single-EXTERNAL equivalent, so a suite that rebuilds specs
        // into an EXTERNAL query cannot express it. Skip such specs here rather than failing in the rebuild.
        assumeFalse(
            "multi-source FROM <dataset> has no single-EXTERNAL equivalent; skipped on EXTERNAL-rebuild backends",
            testCase.datasetSources.size() > 1
        );

        // A declared schema is a property of the DATASET, not of a query: EXTERNAL has no clause that carries one, and
        // copying the directive's reserved `mappings` key into an EXTERNAL WITH would fail option validation instead of
        // declaring anything, so such a case is skipped rather than rebuilt.
        //
        // Live, not dormant: ParquetRsFormatSpecIT globs external-*.csv-spec AND forces the EXTERNAL rebuild, and
        // external-declared-schema.csv-spec is now such a file in which every case declares a schema. This skip is
        // what keeps those cases off that suite -- a declaration has no EXTERNAL-query equivalent to rebuild into.
        assumeFalse(
            "a declared schema cannot be expressed as an EXTERNAL ... WITH query; skipped on EXTERNAL-rebuild backends",
            declaresMappings()
        );

        // Pick the Azure URI form once per test so wildcard expansion sees a single, consistent form.
        useAzureHadoopForm = storageBackend == StorageBackend.AZURE && randomBoolean();

        // Either a raw-EXTERNAL spec with no dataset: directive (the Iceberg holdout, left unchanged) or a
        // holdout suite whose reader cannot be addressed via FROM <dataset> (parquet-rs — see
        // forceExternalRebuild()): in the latter case rebuild the EXTERNAL query from the single dataset
        // directive so the suite's reader override still applies. A spec with no directive is returned as-is.
        String query = rebuildExternalFromDatasets(testCase.query);

        if (referencesGlobLayout(query)) {
            // HTTP does not support directory listing, so skip multi-file/Hive-partitioned glob tests
            assumeTrue("HTTP backend does not support multi-file glob patterns", storageBackend != StorageBackend.HTTP);
        }

        // Transform templates like {{employees}} to actual paths
        query = transformTemplates(query);

        // Inject endpoint and credentials for the raw-EXTERNAL path (Iceberg).
        if (isExternalQuery(query)) {
            query = switch (storageBackend) {
                case StorageBackend.S3 -> s3Fixture.injectParams(query);
                case StorageBackend.GCS -> gcsFixture.injectParams(query);
                case StorageBackend.AZURE -> azureFixture.injectParams(query);
                default -> query;
            };
            query = injectReaderParam(query);
        }

        logger.debug("Transformed query for {} backend: {}", storageBackend, query);
        runColdThenWarm(query, isExternalQuery(query) && testCase.expectedDocumentsFound == null);
    }

    /**
     * Runs {@code query} once (cold) and, when {@code warmPass} is set, a second time (warm) against the
     * identical expected results.
     * <p>
     * The warm pass exercises the cache on EVERY external/dataset spec test, for every format and codec
     * that extends this base. The cold run reconciles the file's statistics into the coordinator's
     * per-file schema cache; the aggregate-metadata pushdown that serves COUNT(*) / MIN / MAX from that
     * cache is a SECOND code path that a single run never touches. Re-running the identical query asserts
     * the warm path, so a cache-only correctness bug (e.g. a COUNT(*) that only doubles on the warm read)
     * fails deterministically here instead of surfacing flakily in CI when the randomized spec order
     * happens to repeat a file against a shared cluster. Callers pass {@code warmPass == false} when the
     * spec pins {@code documents_found}, because the warm run short-circuits to zero scanned documents and
     * so cannot match the cold scan count. The schema cache is per-coordinator: on a single-node IT the
     * warm run always hits it; on a multi-node IT the second run may land on another coordinator and
     * re-scan (a coverage gap, never a wrong answer). The deterministic ExternalNdJsonMultiScanPushdownIT
     * is the guaranteed warm-path guard regardless of routing.
     */
    private void runColdThenWarm(String query, boolean warmPass) throws Throwable {
        doTest(query);
        if (warmPass) {
            doTest(query);
        }
    }

    /**
     * Registers the {@code data_source} (once per backend) and every declared {@code dataset}, then runs
     * the spec's {@code FROM <name>} query verbatim — cold then warm via {@link #runColdThenWarm}, the
     * same idiom the raw-EXTERNAL flow uses. Each source's resource template is resolved to the backend
     * URI exactly as the EXTERNAL path resolves it. The format reader is selected by the resource's file
     * extension against the readers the cluster's installed datasource plugin registers; the dataset model
     * exposes no {@code reader}/{@code format} selector, so a reader that registers no extension (e.g. the
     * parquet-rs native reader) is not reachable on this path.
     * <p>
     * Skipped (rather than failed) on a cluster that lacks {@code dataset_in_from_command}: that
     * capability gates resolving {@code FROM <dataset>} in {@code POST /_query}, which is what this path
     * exercises, independently of the spec's static {@code required_capability} lines.
     */
    private void runDatasetMode() throws Throwable {
        assumeTrue(
            "FROM <dataset> requires the [dataset_in_from_command] capability",
            hasCapabilities(client(), List.of(EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.capabilityName()))
        );
        // HTTP cannot list a directory, so multi-file/Hive-partitioned glob datasets cannot be resolved
        // over it; skip those on the HTTP backend (the glob lives in the dataset's resource template).
        for (DatasetSource source : testCase.datasetSources) {
            if (referencesGlobLayout(source.resource())) {
                assumeTrue("HTTP backend does not support multi-file glob patterns", storageBackend != StorageBackend.HTTP);
            }
        }
        if (declaresMappings()) {
            assumeTrue(
                "a declared schema requires the [" + EsqlDataSourcesCapabilities.DATASET_DECLARED_SCHEMA + "] capability",
                clusterSupportsDeclaredSchema()
            );
        }
        String dataSourceName = ensureDataSourceForBackend();
        for (DatasetSource source : testCase.datasetSources) {
            String resource = transformTemplates(source.resource());
            DatasetRegistry.ensureDataset(client(), source.name(), dataSourceName, resource, withJsonForSource(source));
        }
        String query = testCase.query;
        logger.debug("Dataset-mode query for {} backend: {}", storageBackend, query);
        runColdThenWarm(query, testCase.expectedDocumentsFound == null);
    }

    /**
     * Lazily registers (and caches) the {@code data_source} pointing at the in-process fixture for the
     * active backend. Every backend authenticates anonymously ({@code auth=anonymous}, or no settings for
     * the unauthenticated HTTP/local sources), so no secret is stored and the suites need no cluster
     * encryption key. The blob credentials, where a real backend would need them, are unnecessary because
     * each fixture serves its blobs without verifying authorization.
     */
    private String ensureDataSourceForBackend() throws IOException {
        return switch (storageBackend) {
            case S3 -> DatasetRegistry.ensureDataSource(
                client(),
                "esql_spec_s3",
                "s3",
                Map.of("endpoint", s3Fixture.getAddress(), "auth", "anonymous")
            );
            case GCS -> DatasetRegistry.ensureDataSource(
                client(),
                "esql_spec_gcs",
                "gcs",
                Map.of("endpoint", gcsFixture.getAddress(), "auth", "anonymous")
            );
            case AZURE -> DatasetRegistry.ensureDataSource(
                client(),
                "esql_spec_azure",
                "azure",
                Map.of("endpoint", azureFixture.getAddress(), "auth", "anonymous")
            );
            case HTTP -> DatasetRegistry.ensureDataSource(client(), "esql_spec_http", "http", Map.of("auth", "anonymous"));
            case LOCAL -> DatasetRegistry.ensureDataSource(client(), "esql_spec_local", "local", Map.of("auth", "anonymous"));
        };
    }

    /**
     * Fail if the matrix does not declare this dataset for this format.
     * <p>
     * Without this the spec resolves to a path no generator ever wrote and the failure arrives
     * as a missing file, or worse as an empty result. The matrix knows whether the cell is
     * absent because no format could carry it or because nobody propagated it, so the message
     * says which.
     */
    private void requireDeclaredCell(String dataset) {
        // The suite's format is a file extension: compressed suites run as "csv.gz" and friends,
        // and the codec dimension does not change which datasets exist.
        String baseFormat = FixtureMatrix.baseFormat(format);
        if (MATRIX.declares(baseFormat, dataset)) {
            return;
        }
        String reason = MATRIX.restrictionReason(dataset);
        StringBuilder message = new StringBuilder("the fixture matrix does not declare dataset [").append(dataset)
            .append("] for format [")
            .append(baseFormat)
            .append("], so this fixture is never generated");
        if (reason == null) {
            message.append(". Declare it in fixture-matrix.properties.");
        } else {
            message.append(". The matrix says: ").append(reason);
        }
        throw new AssertionError(message.toString());
    }

    /**
     * Override to change the base directory within the resource tree where single-file fixtures live.
     * Defaults to {@code "standalone"}. Subclasses testing compressed Parquet fixtures can override
     * this to point at codec-specific directories (e.g. {@code "standalone-snappy"}).
     */
    protected String fixturesBase() {
        return FIXTURES_BASE;
    }

    /**
     * Override to change the base directory within the resource tree where multi-file split fixtures
     * live (template {@code {{x_multifile_split}}}). Defaults to the directory the fixture matrix
     * declares for that layout. Subclasses testing codec-compressed multi-file fixtures override this
     * to point at codec-specific directories (e.g. {@code "multifile_split-gzip"}).
     */
    protected String multifileSplitDir() {
        return MATRIX.layout(MULTIFILE_SPLIT_LAYOUT).dir();
    }

    /**
     * Override to specify a reader implementation for the EXTERNAL query.
     * When non-null, a {@code "reader": "<name>"} parameter is injected into the WITH clause.
     *
     * @return the reader name (e.g. "java", "parquet-rs"), or null for the default reader
     */
    protected String readerName() {
        return null;
    }

    /**
     * Whether this suite must drive its specs through the raw {@code EXTERNAL} command rather than the
     * {@code FROM <dataset>} path, rebuilding the EXTERNAL query from each spec's {@code dataset:} directive.
     * <p>
     * Defaults to {@code false}: every dataset-backed suite runs via {@code FROM <dataset>}. The sole opt-in
     * is the parquet-rs suite: the parquet-rs native reader registers no file extension and the dataset model
     * exposes no {@code reader}/{@code format} selector ({@code Dataset} carries only
     * {@code data_source}/{@code resource}/{@code settings}, and settings are validated against the format's
     * config keys), so parquet-rs is reachable only via {@code EXTERNAL ... WITH "reader": "parquet-rs"}. It is
     * therefore a sanctioned EXTERNAL holdout, like gRPC/Flight and Iceberg.
     */
    protected boolean forceExternalRebuild() {
        return false;
    }

    /** Whether any of this spec's {@code dataset:} directives declares a schema. Memoized: asked at two guard sites. */
    private boolean declaresMappings() {
        if (declaresMappingsMemo == null) {
            declaresMappingsMemo = testCase.datasetSources.stream().anyMatch(source -> DatasetRegistry.declaresMappings(source.withJson()));
        }
        return declaresMappingsMemo;
    }

    /**
     * Memoized support for a declared schema on {@code PUT /_query/dataset/<name>}.
     * <p>
     * The declared-schema capability is advertised on the dataset PUT/GET routes, NOT on {@code POST /_query}, so a
     * spec file cannot gate on it: a {@code required_capability:} line resolves against the query endpoint and would
     * skip such a case on every cluster, forever. The harness therefore asks the dataset route directly.
     * <p>
     * Cached because the sibling {@code dataset_in_from_command} check resolves through {@code hasCapabilities},
     * which caches, and an uncached {@code GET _capabilities} per declaring test would add a round trip to each of
     * them. Reset in the same {@code @AfterClass} that clears the registry's caches, so a later suite in the JVM
     * fork cannot inherit a verdict about a cluster it is not talking to.
     */
    private static volatile Boolean declaredSchemaSupported;

    private static boolean clusterSupportsDeclaredSchema() throws IOException {
        // Racy single-check: read the volatile field ONCE into a local. Reading it twice would let the
        // @AfterClass reset land between the assignment and the return and unbox null. A duplicate probe is
        // harmless -- the capability is immutable for a cluster's lifetime.
        Boolean supported = declaredSchemaSupported;
        if (supported == null) {
            supported = clusterHasCapability(
                client(),
                "PUT",
                "/_query/dataset/{name}",
                List.of(),
                List.of(EsqlDataSourcesCapabilities.DATASET_DECLARED_SCHEMA)
            ).orElse(false);
            declaredSchemaSupported = supported;
        }
        return supported;
    }

    /**
     * Rebuilds a single-source {@code FROM <dataset>} spec into the equivalent {@code EXTERNAL "<resource>"
     * WITH {...}} query so a {@link #forceExternalRebuild() holdout} suite can run it via the EXTERNAL command.
     * A spec with no {@code dataset:} directive (a raw-EXTERNAL spec, e.g. Iceberg) is returned unchanged.
     * Multi-source FROM has no single-EXTERNAL equivalent and is rejected.
     */
    protected final String rebuildExternalFromDatasets(String query) {
        List<DatasetSource> sources = testCase.datasetSources;
        if (sources.isEmpty()) {
            return query;
        }
        if (sources.size() > 1) {
            throw new AssertionError(
                "Cannot rebuild a single EXTERNAL query for ["
                    + sources.size()
                    + "] dataset sources; multi-source FROM <dataset> has no EXTERNAL equivalent yet: "
                    + query
            );
        }
        DatasetSource source = sources.get(0);
        int pipe = FixtureUtils.findFirstPipeAfterExternal(query);
        String tail = pipe < 0 ? "" : " " + query.substring(pipe);
        // source.resource() is decoded (quotes/escapes resolved by the parser); re-escape it back into the
        // EXTERNAL string literal so a resource containing a backslash or quote round-trips correctly.
        String literal = source.resource().replace("\\", "\\\\").replace("\"", "\\\"");
        StringBuilder external = new StringBuilder("EXTERNAL \"").append(literal).append("\"");
        // Apply the same WITH JSON the FROM path uses (adds trim_spaces for the column-aligned csv/tsv
        // fixtures) so the EXTERNAL-holdout path reads them identically.
        String withJson = withJsonForSource(source);
        if (withJson != null) {
            external.append(" WITH ").append(withJson);
        }
        external.append(tail);
        return external.toString();
    }

    /**
     * The {@code WITH}-clause JSON applied to a dataset source, both when registering the dataset
     * ({@link #runDatasetMode()}) and when rebuilding an {@code EXTERNAL} query
     * ({@link #rebuildExternalFromDatasets}).
     * <p>
     * Passed through as one blob: the reserved {@code mappings} key (a declared schema) is split out of it by
     * {@code DatasetRegistry}, not here. The injection below therefore has to land at the TOP level of the object even
     * when a nested {@code mappings} object is its last entry.
     * <p>
     * The CSV/TSV test fixtures (employees.csv, books.csv, ...) are column-aligned with padding spaces for
     * readability, so their expected spec values assume trimming. The reader default is now no-trim (RFC
     * 4180 — spaces are part of a field), so read these aligned fixtures with {@code trim_spaces: true} to
     * keep the expected values valid. Real-world no-trim fidelity is covered by CsvFormatReaderTests unit
     * tests; a directive that sets {@code trim_spaces} explicitly is left untouched (so a spec can still
     * exercise the no-trim default end to end).
     */
    /**
     * The directive settings the running vector pins, keyed by their {@code WITH} key.
     *
     * <p>Empty by default, so a suite that has not been moved onto generated vectors behaves exactly as
     * before. A suite driven by {@link org.elasticsearch.xpack.esql.datasources.fixtures.FixtureDimensions}
     * overrides this with the directive-bound slots of its vector that sit off their declared default.
     */
    protected Map<String, String> vectorSettings() {
        return Map.of();
    }

    private String withJsonForSource(DatasetSource source) {
        // Memoized per source: injectTrimSpaces parses the JSON to decide whether the directive already sets
        // trim_spaces, and this is asked once per registration and again when the query is built.
        return withJsonMemo.computeIfAbsent(source, s -> {
            // format is the base format or a codec-suffixed variant ("csv", "csv.gz", "tsv.zstd", ...). Other
            // formats (parquet, ...) reject the trim_spaces key, so only the csv/tsv backends read the
            // column-aligned fixtures with trimming; the shared injector adds the key.
            boolean csvOrTsv = format.equals("csv") || format.startsWith("csv.") || format.equals("tsv") || format.startsWith("tsv.");
            String json = csvOrTsv ? injectMultiValueSyntax(injectTrimSpaces(s.withJson()), s.resource()) : s.withJson();
            // Then whatever the running vector pins. A directive-bound dimension at its default injects
            // nothing -- omission IS the default -- so an unvaried suite produces byte-identical JSON to
            // before, which is what lets vectors be introduced one dimension at a time.
            for (Map.Entry<String, String> setting : vectorSettings().entrySet()) {
                json = injectSetting(json, setting.getKey(), setting.getValue());
            }
            return json;
        });
    }

    /**
     * Adds {@code "trim_spaces": true} to a dataset directive's {@code WITH} JSON, unless the directive already sets
     * that SETTING. Whether it does is decided by parsing rather than by matching the raw text: a directive may now
     * carry a nested declared schema, and a same-named key inside {@code mappings} would otherwise suppress the
     * injection and read the column-aligned fixtures untrimmed. Placement stays textual — {@code withJson} is
     * parser-guaranteed to be a brace-delimited object or {@code null}, so {@code lastIndexOf('}')} is always the
     * structural closer, outside any nested object.
     */
    /**
     * Adds {@code "multi_value_syntax": "brackets"} for a source whose fixtures were WRITTEN with bracket
     * multi-values, unless the directive already sets it.
     *
     * <p>Per source, not per suite. employees.csv carries bracket multi-values on all 100 rows, so an
     * RFC-4180 reader splitting on commas misaligns every column of anything derived from it; employees_no_mv
     * carries none. A single spec can read both, so there is no suite-wide answer -- and injecting brackets
     * everywhere would retire the coverage of the {@code none} default, which is what real users get.
     *
     * <p>The dialect comes from the declaration via {@link FixtureMatrix#writeDialectForTemplate}, keyed on
     * the template the directive names. It is a property of the authored data, and checkFixtureDialect fails
     * if the declaration and the CSV disagree.
     */
    static String injectMultiValueSyntax(String withJson, String resource) {
        String template = templateNameIn(resource);
        if (template == null || "brackets".equals(MATRIX.writeDialectForTemplate(template)) == false) {
            return withJson;
        }
        return injectSetting(withJson, "multi_value_syntax", "brackets");
    }

    /**
     * Adds one setting to a dataset directive's {@code WITH} JSON, unless the directive already declares it.
     *
     * <p>Whether it does is decided by parsing rather than by matching raw text: a directive may carry a
     * nested declared schema, and a same-named key inside {@code mappings} would otherwise suppress the
     * injection. Placement stays textual -- {@code withJson} is parser-guaranteed to be a brace-delimited
     * object or {@code null}, so {@code lastIndexOf('}')} is always the structural closer.
     *
     * <p>This is the seam every generated vector reaches: a dimension declared to bind as a directive
     * becomes a key here, so adding one to the declaration needs no new injector.
     */
    static String injectSetting(String withJson, String key, String value) {
        if (DatasetRegistry.declaresSetting(withJson, key)) {
            return withJson;
        }
        String entry = "\"" + key + "\": \"" + value + "\"";
        if (withJson == null) {
            return "{" + entry + "}";
        }
        int close = withJson.lastIndexOf('}');
        String head = withJson.substring(0, close).trim();
        String separator = head.endsWith("{") ? "" : ", ";
        return head + separator + entry + withJson.substring(close);
    }

    /** The {@code {{template}}} a dataset directive's resource names, or null when it names none. */
    private static String templateNameIn(String resource) {
        if (resource == null) {
            return null;
        }
        Matcher m = TEMPLATE_PATTERN.matcher(resource);
        return m.find() ? m.group(1) : null;
    }

    static String injectTrimSpaces(String withJson) {
        if (DatasetRegistry.declaresSetting(withJson, "trim_spaces")) {
            return withJson;
        }
        if (withJson == null) {
            return "{\"trim_spaces\": true}";
        }
        int close = withJson.lastIndexOf('}');
        String head = withJson.substring(0, close).trim();
        return head + (head.endsWith("{") ? "" : ", ") + "\"trim_spaces\": true}";
    }

    /**
     * Inject the reader parameter into the query's WITH clause.
     * If a WITH clause already exists, the reader param is appended; otherwise a new WITH clause is added.
     */
    private String injectReaderParam(String query) {
        String reader = readerName();
        if (reader == null) {
            return query;
        }
        String readerEntry = "\"reader\": \"" + reader + "\"";
        int pipeIndex = FixtureUtils.findFirstPipeAfterExternal(query);
        // Only look for WITH { in the EXTERNAL part (before the first pipe),
        // so we don't accidentally match a RERANK/COMPLETION WITH clause.
        String externalPart = pipeIndex == -1 ? query : query.substring(0, pipeIndex);
        int withIndex = externalPart.indexOf("WITH {");
        if (withIndex >= 0) {
            int closingBrace = findClosingBrace(query, query.indexOf('{', withIndex));
            assert closingBrace >= 0 : "Malformed WITH clause in query: " + query;
            return query.substring(0, closingBrace) + ", " + readerEntry + query.substring(closingBrace);
        }
        if (pipeIndex == -1) {
            return query + " WITH { " + readerEntry + " }";
        }
        return query.substring(0, pipeIndex).trim() + " WITH { " + readerEntry + " } " + query.substring(pipeIndex);
    }

    /**
     * Finds the closing brace matching the opening brace at {@code openIndex},
     * skipping over quoted strings so braces inside string values are ignored.
     * <p>
     * Assumes ES|QL string-literal syntax: only {@code "..."} (with backslash escapes) is recognised.
     * Single-quoted strings are not part of the ES|QL grammar so they are not handled here. Triple-quoted
     * strings ({@code """..."""}) are not specifically parsed either; they happen to work in the current
     * state machine because consecutive quotes toggle the {@code inQuotes} flag, but adding
     * {@code """}-aware handling would be required if a spec ever embeds {@code }} inside a triple-quoted
     * value. No EXTERNAL csv-spec uses that form today.
     */
    private static int findClosingBrace(String query, int openIndex) {
        int depth = 0;
        boolean inQuotes = false;
        for (int i = openIndex; i < query.length(); i++) {
            char c = query.charAt(i);
            if (inQuotes) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inQuotes = false;
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
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

    /**
     * The fixture matrix. The suffixes and directories below are read from it rather than
     * written down here, so the layout a generator writes into and the layout a spec asks for
     * cannot drift apart -- naming a layout the declaration does not know about fails on the
     * spot instead of resolving to a path nothing ever wrote.
     */
    private static final FixtureMatrix MATRIX = FixtureMatrix.get();

    /** The one layout whose directory a subclass may redirect to a codec-specific variant. */
    private static final String MULTIFILE_SPLIT_LAYOUT = "multifile_split";

    /**
     * True when the given text references a layout the fixture matrix declares as a GLOB layout (anything
     * other than {@code standalone}). Derived from the declaration rather than from hard-coded layout names,
     * so a layout added to {@code fixture-matrix.properties} is covered without editing this class -- which
     * is the whole point of the declaration owning the convention.
     *
     * <p>Matching is on the bare suffix, which needs no precedence reasoning: every glob layout contributes
     * its own suffix, so {@code _hive_shadow} is matched by its own entry rather than by being a superstring
     * of {@code _hive}.
     */
    private static boolean referencesGlobLayout(String text) {
        for (FixtureMatrix.Layout layout : MATRIX.layouts()) {
            if (layout.isStandalone() == false && text.contains(layout.suffix())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolve a template name to an actual path based on storage backend and format.
     *
     * @param templateName the template name (e.g., "employees", "employees_multifile", or "employees_multifile_ubn")
     * @return the resolved path
     */
    private String resolveTemplatePath(String templateName) {
        FixtureMatrix.Layout layout = MATRIX.layoutFor(templateName);
        String relativePath;
        if (layout.isStandalone()) {
            // A single file named after the dataset -- the one layout that is per-dataset, so the only
            // one whose absence is checkable HERE, from the template name alone.
            //
            // Not the only way a cell can be absent, though: a glob layout can be declared empty for a
            // format (layout.<name>.sources.<format> = , with a reason), and a suite can route a spec
            // naming a layout its format never generates. Those are caught by checkFixtureCoverage
            // against the declaration rather than at resolve time, because the template alone does not
            // say which format is reading it.
            requireDeclaredCell(templateName);
            relativePath = fixturesBase() + "/" + templateName + "." + format;
        } else {
            // Subclasses testing codec-compressed multi-file fixtures override multifileSplitDir()
            // to route to codec-specific directories (e.g. "multifile_split-gzip").
            String dir = layout.name().equals(MULTIFILE_SPLIT_LAYOUT) ? multifileSplitDir() : layout.dir();
            relativePath = dir + "/" + layout.glob() + "." + format;
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
                    return resolveLocalUri(localFixturesPath, relativePath);
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

    /**
     * Build a {@code file://} URI for a relative path under {@code base}, tolerating glob
     * characters like {@code *} that are illegal in filesystem path components on Windows.
     * <p>
     * {@link Path#resolve(String)} delegates to the filesystem provider, which on Windows
     * (NTFS) rejects {@code *} because it is a reserved filename character. The downstream
     * local file loader expands the glob itself, so the URI we produce here only needs to
     * be a syntactically valid {@code file://} URI - we don't have to round-trip through
     * {@link Path}. We split the relative path on the first glob meta-character, resolve
     * the literal prefix via {@link Path#resolve(String)} (which is portable), and append
     * the glob suffix to the resulting URI as-is. {@code *} is a valid URI sub-delim
     * character per RFC 3986 and does not require percent-encoding.
     */
    static String resolveLocalUri(Path base, String relativePath) {
        int globIdx = indexOfGlobMeta(relativePath);
        if (globIdx < 0) {
            return base.resolve(relativePath).toUri().toString();
        }
        // Find the last path separator before the glob meta-character so the literal portion
        // we feed to Path.resolve() contains no glob characters.
        int splitIdx = relativePath.lastIndexOf('/', globIdx);
        if (splitIdx < 0) {
            // Glob meta-character in the first path segment - resolve the base itself.
            return appendGlobSuffix(base.toUri().toString(), relativePath);
        }
        String literalPrefix = relativePath.substring(0, splitIdx);
        String globSuffix = relativePath.substring(splitIdx + 1);
        Path literalParent = base.resolve(literalPrefix);
        return appendGlobSuffix(literalParent.toUri().toString(), globSuffix);
    }

    private static int indexOfGlobMeta(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '*' || c == '?') {
                return i;
            }
        }
        return -1;
    }

    private static String appendGlobSuffix(String baseUri, String suffix) {
        return baseUri.endsWith("/") ? baseUri + suffix : baseUri + "/" + suffix;
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
