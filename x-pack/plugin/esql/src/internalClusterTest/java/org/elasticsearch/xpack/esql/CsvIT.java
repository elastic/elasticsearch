/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.ingest.geoip.GeoIpTestUtils;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.useragent.UserAgentPlugin;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.ml.job.categorization.FirstLineWithLettersCharFilter;
import org.elasticsearch.xpack.ml.job.categorization.FirstLineWithLettersCharFilterFactory;
import org.elasticsearch.xpack.ml.job.categorization.FirstNonBlankLineCharFilter;
import org.elasticsearch.xpack.ml.job.categorization.FirstNonBlankLineCharFilterFactory;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizer;
import org.elasticsearch.xpack.ml.job.categorization.MlClassicTokenizerFactory;
import org.elasticsearch.xpack.ml.job.categorization.MlStandardTokenizer;
import org.elasticsearch.xpack.ml.job.categorization.MlStandardTokenizerFactory;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.CsvTestUtils.assumeFalseLogging;
import static org.elasticsearch.xpack.esql.CsvTestUtils.assumeTrueLogging;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.CSV_DATASET;
import static org.elasticsearch.xpack.esql.CsvTestsDataLoader.INFERENCE_CONFIGS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * CSV-based integration testing.
 * This test picks up *.csv-specs and run them against `InternalTestCluster`.
 * Unlike CsvTests this does not mock esql query execution infra and relies on real components.
 * InternalTestCluster` reuses current jvm. This enables debugging all scenarios from IDE.
 * Test data is loaded lazily in order to facilitate faster startup when running/debugging individual test cases.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class CsvIT extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(CsvIT.class);
    private static final EsqlCapabilities ENABLED_CAPS = EsqlCapabilities.capabilities(TEST_FUNCTION_REGISTRY, false);
    private static final EsqlCapabilities ALL_CAPS = EsqlCapabilities.capabilities(TEST_FUNCTION_REGISTRY, true);
    private static final QueryPragmas ALL_PRAGMAS = new QueryPragmas(Settings.EMPTY);
    private static final int BULK_INDEX_BATCH_SIZE = 10_000;

    private static InternalTestCluster cluster;
    private static String currentGroupName = null;

    /**
     * Hook for tests that want to load datasets with a transformed mapping and/or transformed source documents,
     * and/or to rewrite the csv-spec query before it is sent to the cluster &mdash; for example to exercise
     * ES|QL behavior with a different field type than the dataset's mapping declares.
     */
    public interface IndexLoadStrategy {
        /**
         * Returns the mapping JSON to use when creating the index for {@code dataset}.
         * The {@code originalMapping} is the mapping after {@link CsvTestsDataLoader#readMappingFile(CsvTestsDataLoader.TestDataset)}
         * has applied {@code TestDataset}-level overrides (e.g. {@code withTypeMapping}, {@code withDynamic}).
         */
        String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) throws IOException;

        /**
         * Returns the index settings to use when creating the index for {@code dataset}.
         */
        Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings);

        /**
         * Returns the document source JSON to bulk-index for {@code dataset}. Called once per CSV row.
         */
        String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) throws IOException;

        /**
         * Returns the ES|QL query to send to the cluster, given the full {@link CsvSpecReader.CsvTestCase}
         * for context (in particular, {@link CsvSpecReader.CsvTestCase#expectedResults} lets a variant
         * recover the expected output column order). {@code testId} is the
         * {@code <fileName>.<testName>} pair the test runner uses to identify the case (the
         * same form {@link org.junit.AssumptionViolatedException} log lines surface) so a
         * variant can consult a per-test silencing registry without threading the test
         * instance into the strategy. The default {@link #IDENTITY_INDEX_LOAD_STRATEGY}
         * returns the original query unchanged regardless of {@code testId}.
         * <p>
         * Implementations may throw {@link org.junit.AssumptionViolatedException} (typically via
         * {@code assumeTrue} / {@code assumeFalse}) to skip the test under this variant when the original
         * query is not relevant for the variant &mdash; for example, when no field that this variant rewrites
         * appears in the query and so re-running the spec would only re-test the unmodified behavior.
         */
        TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase);

        record TransformedQuery(String query, Settings extraPragmas) {}

        /**
         * Transforms the expected results loaded from the csv-spec entry before they are compared
         * against the actual query output.
         */
        ExpectedResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, ExpectedResults expected);

        /**
         * Called once after the index for {@code dataset} has been fully populated.
         */
        default void afterIndexLoaded(CsvTestsDataLoader.TestDataset dataset, Client client) throws IOException {}
    }

    public static final IndexLoadStrategy IDENTITY_INDEX_LOAD_STRATEGY = new IndexLoadStrategy() {
        @Override
        public String transformMapping(CsvTestsDataLoader.TestDataset dataset, String originalMapping) {
            return originalMapping;
        }

        @Override
        public Settings transformSettings(CsvTestsDataLoader.TestDataset dataset, Settings settings) {
            return settings;
        }

        @Override
        public String transformDocument(CsvTestsDataLoader.TestDataset dataset, String originalDocumentJson) {
            return originalDocumentJson;
        }

        @Override
        public TransformedQuery transformQuery(String testId, CsvSpecReader.CsvTestCase testCase) {
            return new TransformedQuery(testCase.query, Settings.EMPTY);
        }

        @Override
        public ExpectedResults transformExpectedResults(String testId, CsvSpecReader.CsvTestCase testCase, ExpectedResults expected) {
            return expected;
        }
    };

    /**
     * Strategy for transforming mappings and documents before they are sent to the test cluster.
     * Defaults to the identity strategy (no transformation). Subclasses can replace this in their
     * own {@link BeforeClass} method, which by JUnit's contract runs after the parent's {@link #setupCluster()}.
     * <p>
     * {@link #setupCluster()} resets the field to {@link #IDENTITY_INDEX_LOAD_STRATEGY} on every run so that
     * a stale subclass strategy from a prior class in the same JVM never leaks into a sibling test class.
     */
    protected static IndexLoadStrategy indexLoadStrategy = IDENTITY_INDEX_LOAD_STRATEGY;

    private final String fileName;
    private final String groupName;
    private final String testName;
    private final Integer lineNumber;
    private final CsvSpecReader.CsvTestCase testCase;
    private final String instructions;

    public CsvIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        this.fileName = fileName;
        this.groupName = groupName;
        this.testName = testName;
        this.lineNumber = lineNumber;
        this.testCase = testCase;
        this.instructions = instructions;
    }

    @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
    public static List<Object[]> readScriptSpec() throws Exception {
        List<URL> urls = classpathResources("/*.csv-spec");
        assertThat("Not enough specs found " + urls, urls, hasSize(greaterThan(0)));
        return SpecReader.readScriptSpec(urls, CsvSpecReader::specParser);
    }

    @BeforeClass
    public static void setupCluster() throws Exception {
        indexLoadStrategy = IDENTITY_INDEX_LOAD_STRATEGY;
        long start = System.currentTimeMillis();
        logger.info("Creating test cluster");
        var nodeDirectory = createTempDir();
        var configDirectory = nodeDirectory.resolve("config");
        createCustomRegexConfig(configDirectory);
        createGeoIpConfig(configDirectory);
        cluster = new InternalTestCluster(
            randomLong(),
            nodeDirectory,
            false,
            true,
            1,
            1,
            "esql_test_cluster",
            new NodeConfigurationSource() {
                @Override
                public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                    return Settings.builder()
                        .put("xpack.security.enabled", false)
                        .put("xpack.license.self_generated.type", "trial")
                        .put("ingest.geoip.downloader.enabled", false)
                        .put(PlannerSettings.PARALLEL_OPERATOR_PROMOTION_THRESHOLD_ROWS.getKey(), 0)
                        .build();
                }

                @Override
                public java.nio.file.Path nodeConfigPath(int nodeOrdinal) {
                    return configDirectory;
                }
            },
            0,
            "node_",
            List.of(
                getTestTransportPlugin(),
                // EncryptionService binding for the always-registered data-source CRUD actions.
                TestEncryptionServicePlugin.class,
                EsqlTestPlugin.class,
                AggregateMetricMapperPlugin.class,
                AnalyticsPlugin.class,
                CommonAnalysisPlugin.class,
                ConstantKeywordMapperPlugin.class,
                EnrichPlugin.class,
                IngestCommonPlugin.class,
                LocalStateInferencePlugin.class,
                MapperExtrasPlugin.class,
                SpatialPlugin.class,
                UnsignedLongMapperPlugin.class,
                UserAgentPlugin.class,
                IngestGeoIpPlugin.class,
                VersionFieldPlugin.class,
                Wildcard.class
            ),
            Function.identity(),
            TEST_ENTITLEMENTS::addEntitledNodePaths
        );
        cluster.beforeTest(random());

        long stop = System.currentTimeMillis();
        logger.info("Started test cluster in {} ms", stop - start);
    }

    @AfterClass
    public static void cleanupCluster() throws IOException {
        cluster.close();
    }

    public final void test() throws Throwable {
        assumeTrueLogging("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));
        assumeFalseLogging(
            "runs in a single cluster/single node mode",
            testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.METADATA_FIELDS_REMOTE_TEST.capabilityName())
        );
        assumeFalseLogging(
            "CSV tests cannot handle EXTERNAL sources (requires QA integration tests)",
            testCase.query.trim().toUpperCase(java.util.Locale.ROOT).startsWith("EXTERNAL")
        );
        assumeFalseLogging(
            "CSV tests cannot handle dataset-backed FROM <dataset> sources (requires QA integration tests)",
            testCase.datasetSources.isEmpty() == false
        );
        assumeTrueLogging(
            "CSV tests don't support remote cluster capability requirements",
            testCase.missingCapabilitiesRemoteCluster.isEmpty()
        );
        CsvTestUtils.checkTestCapabilities(ALL_CAPS, ENABLED_CAPS, testCase.requiredCapabilities);
        CsvTestUtils.checkTestCapabilities(ALL_CAPS, ENABLED_CAPS, testCase.requiredCapabilitiesLocalCluster);
        CsvTestUtils.checkMissingTestCapabilities(ENABLED_CAPS, testCase.missingCapabilitiesLocalCluster);
        CsvTestUtils.checkPragma(testCase.pragmas);

        currentGroupName = groupName;
        // verify no prior failures
        indices.ensureNoFailures();
        enrich.ensureNoFailures();
        inference.ensureNoFailures();
        views.ensureNoFailures();

        IndexLoadStrategy.TransformedQuery transformed = indexLoadStrategy.transformQuery(groupName + "." + testName, testCase);
        EsqlQueryRequest request = syncEsqlQueryRequest(transformed.query());
        if (testCase.requestTimeRangeGte != null && testCase.requestTimeRangeGte.isEmpty() == false) {
            request.filter(new RangeQueryBuilder("@timestamp").gte(testCase.requestTimeRangeGte).lte(testCase.requestTimeRangeLte));
        }

        Settings.Builder pragmaSettings = Settings.builder();
        if (randomBoolean()) {
            pragmaSettings.put("max_concurrent_shards_per_node", randomBoolean() ? 1 : between(2, 10));
        }
        pragmaSettings.put(transformed.extraPragmas());
        testCase.pragmas.forEach(pragmaSettings::put);
        if (pragmaSettings.build().isEmpty() == false) {
            request.acceptedPragmaRisks(true).pragmas(new QueryPragmas(pragmaSettings.build()));
        }

        var listener = new ResponseListener(cluster.getInstance(TransportService.class).getThreadPool());
        cluster.client().execute(EsqlQueryAction.INSTANCE, request, listener);
        // Using a longer timeout here as test infrastructure might populate data lazily while request is in progress.
        try (var response = listener.actionGet(5, TimeUnit.MINUTES)) {
            assertFalse("response must not be partial: " + response.getExecutionInfo(), response.isPartial());
            ExpectedResults expected = indexLoadStrategy.transformExpectedResults(
                groupName + "." + testName,
                testCase,
                loadCsvSpecValues(testCase.expectedResults)
            );
            ActualResults actual = new ActualResults(
                response.zoneId(),
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                response.columns().stream().map(column -> CsvTestUtils.Type.asType(column.type().nameUpper())).toList(),
                response.columns().stream().map(ColumnInfoImpl::type).toList(),
                response.pages(),
                Map.of()
            );

            CsvAssert.assertMetadata(expected, actual.columnNames(), actual.columnTypes(), logger);
            CsvAssert.assertDataWithValueConverter(
                expected,
                actual.values(),
                testCase.ignoreOrder,
                false,
                false,
                logResults() ? logger : null
            );
            var warnings = listener.warnings.stream()
                .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
                .filter(w -> w.startsWith("No limit defined, adding default limit of") == false)
                .toList();
            testCase.assertWarnings(false).assertWarnings(warnings, null);
            CsvAssert.assertDocumentsFound(testCase.expectedDocumentsFound, response.documentsFound());
        } catch (Throwable t) {
            t.setStackTrace(prependSpec(t.getStackTrace()));
            throw t;
        }
    }

    private StackTraceElement[] prependSpec(StackTraceElement[] original) {
        StackTraceElement[] copy = new StackTraceElement[original.length + 1];
        copy[0] = new StackTraceElement(getClass().getName(), groupName + "." + testName, fileName, lineNumber);
        System.arraycopy(original, 0, copy, 1, original.length);
        return copy;
    }

    public boolean logResults() {
        return false;
    }

    public static class EsqlTestPlugin extends EsqlPlugin implements NetworkPlugin, AnalysisPlugin {
        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null));
        }

        @Override
        public void loadExtensions(ExtensionLoader loader) {
            // nothing, else it would clash with super's SPI discoverer, which adds data source plugins
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter.Simple() {
                @Override
                public int order() {
                    return Integer.MAX_VALUE;
                }

                @Override
                protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
                    switch (action) {
                        case EsqlQueryAction.NAME -> loadViews();
                        case EsqlResolveFieldsAction.NAME -> loadIndices((FieldCapabilitiesRequest) request);
                        case GetInferenceModelAction.NAME -> loadInference((GetInferenceModelAction.Request) request);
                    }
                    return true;
                }
            });
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> handler
                ) {
                    return switch (action) {
                        case EnrichPolicyResolver.RESOLVE_ACTION_NAME -> (request, channel, task) -> {
                            loadEnrichPolicy((EnrichPolicyResolver.LookupRequest) request);
                            handler.messageReceived(request, channel, task);
                        };
                        default -> handler;
                    };
                }
            });
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<CharFilterFactory>> getCharFilters() {
            return Map.of(
                FirstNonBlankLineCharFilter.NAME,
                FirstNonBlankLineCharFilterFactory::new,
                FirstLineWithLettersCharFilter.NAME,
                FirstLineWithLettersCharFilterFactory::new
            );
        }

        @Override
        public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
            return Map.of(
                MlClassicTokenizer.NAME,
                MlClassicTokenizerFactory::new,
                MlStandardTokenizer.NAME,
                MlStandardTokenizerFactory::new
            );
        }
    }

    private static void loadViews() {
        // TODO We should instead load views once and never unload them
        if ("views".equals(currentGroupName) || "approximation".equals(currentGroupName) || "unmapped-load".equals(currentGroupName)) {
            CsvTestsDataLoader.VIEW_CONFIGS.forEach((name, view) -> {
                if (view.requiredCapabilities().stream().allMatch(EsqlCapabilities.Cap::isEnabled)) {
                    views.maybeLoad(name, view);
                }
            });
        } else {
            views.unloadAll();
        }
    }

    private static void loadIndices(FieldCapabilitiesRequest request) {
        Stream.of(request.indices()).flatMap(pattern -> {
            assert pattern.contains("<") == false : "Date-math is not supported in test";
            if (pattern.contains("*")) {
                if (pattern.equals("*")) {
                    switch (currentGroupName) {
                        // Temporarily allow a few so they have time to migrate away
                        case "enrich", "inlinestats", "limit", "lookup-join" -> logger.warn("stop using FROM *");
                        // Views tests need FROM * with exclusions to test wildcard view resolution (e.g. FROM *,-employees*)
                        case "views" -> logger.info("FROM * used in views test");
                        default -> throw new IllegalStateException(
                            "FROM * is not allowed in csv-spec tests because it makes them brittle. We add new data sets frequently."
                        );
                    }
                    return CSV_DATASET.values().stream();
                }
                if (pattern.endsWith("*") == false) {
                    throw new IllegalStateException("CsvIT only supports suffix patterns but got: " + pattern);
                }
                String prefix = pattern.substring(pattern.startsWith("-") ? 1 : 0, pattern.length() - 1);
                if (prefix.length() < 3) {
                    throw new IllegalStateException(
                        "FROM pattern* may not be short in csv-spec tests because it makes them brittle. We add new data sets frequently."
                    );
                }
                return CSV_DATASET.values().stream().filter(ds -> ds.indexName().startsWith(prefix));
            } else {
                return Stream.of(CSV_DATASET.get(pattern));
            }
        })
            .filter(Objects::nonNull)
            .filter(resource -> resource.requiredCapabilities().stream().allMatch(EsqlCapabilities.Cap::isEnabled))
            .forEach(resource -> indices.maybeLoad(resource.indexName(), resource));
    }

    private static void loadInference(GetInferenceModelAction.Request request) {
        var inferenceId = request.getInferenceEntityId();
        if (!Objects.equals(inferenceId, "*")) {
            inference.maybeLoad(inferenceId, INFERENCE_CONFIGS.get(inferenceId));
        }
    }

    private static void loadEnrichPolicy(EnrichPolicyResolver.LookupRequest request) {
        for (var name : request.policyNames) {
            enrich.maybeLoad(name, CsvTestsDataLoader.ENRICH_POLICIES.get(name));
        }
    }

    private static ResourceLoader<CsvTestsDataLoader.TestDataset> indices = new ResourceLoader<>() {
        @Override
        protected void load(CsvTestsDataLoader.TestDataset dataset) throws IOException {
            logger.info("Loading dataset [{}]", dataset.indexName());
            for (String inferenceId : dataset.inferenceEndpoints()) {
                inference.maybeLoad(inferenceId, INFERENCE_CONFIGS.get(inferenceId));
            }
            String mapping = indexLoadStrategy.transformMapping(dataset, CsvTestsDataLoader.readMappingFile(dataset));
            Settings settings = indexLoadStrategy.transformSettings(dataset, dataset.loadSettings());
            assertAcked(cluster.client().admin().indices().prepareCreate(dataset.indexName()).setMapping(mapping).setSettings(settings));
            if (dataset.dataFileName() != null) {
                var bulk = cluster.client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

                for (var document : CsvTestsDataLoader.readCsvDocuments(dataset.streamData(), dataset.allowSubFields())) {
                    String source = indexLoadStrategy.transformDocument(dataset, document.json().toString());
                    var indexRequestBuilder = cluster.client()
                        .prepareIndex(dataset.indexName())
                        .setId(document.id())
                        .setSource(source, XContentType.JSON);
                    if (document.slice() != null) {
                        indexRequestBuilder.setRouting(document.slice());
                        indexRequestBuilder.setRoutingFromSlice(true);
                    }

                    bulk.add(indexRequestBuilder);
                    if (bulk.numberOfActions() >= BULK_INDEX_BATCH_SIZE) {
                        var result = bulk.get();
                        assertFalse(
                            "Must load dataset [" + dataset.indexName() + "] successfully: " + result.buildFailureMessage(),
                            result.hasFailures()
                        );
                        bulk = cluster.client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    }
                }
                if (bulk.numberOfActions() > 0) {
                    var result = bulk.get();
                    assertFalse(
                        "Must load dataset [" + dataset.indexName() + "] successfully: " + result.buildFailureMessage(),
                        result.hasFailures()
                    );
                }
            }
            indexLoadStrategy.afterIndexLoaded(dataset, cluster.client());
        }
    };

    private static ResourceLoader<CsvTestsDataLoader.EnrichConfig> enrich = new ResourceLoader<>() {
        @Override
        protected void load(CsvTestsDataLoader.EnrichConfig policy) throws IOException {
            logger.info("Creating policy [{}]", policy.policyFileName());
            var p = EnrichPolicy.fromXContent(
                JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, policy.streamPolicy())
            );
            for (var index : p.getIndices()) {
                indices.maybeLoad(index, CSV_DATASET.get(index));
            }
            assertAcked(
                cluster.client()
                    .execute(
                        PutEnrichPolicyAction.INSTANCE,
                        new PutEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy.policyName(), p)
                    )
            );
            var response = cluster.client()
                .execute(
                    ExecuteEnrichPolicyAction.INSTANCE,
                    new ExecuteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, policy.policyName())
                )
                .actionGet();
            assertTrue(response.getStatus().isCompleted());
        }
    };

    private static ResourceLoader<CsvTestsDataLoader.InferenceConfig> inference = new ResourceLoader<>() {
        @Override
        protected void load(CsvTestsDataLoader.InferenceConfig inference) {
            logger.info("Loading inference [{}]", inference.id());
            cluster.client()
                .execute(
                    PutInferenceModelAction.INSTANCE,
                    new PutInferenceModelAction.Request(
                        inference.type(),
                        inference.id(),
                        new BytesArray(inference.loadConfig()),
                        XContentType.JSON,
                        TEST_REQUEST_TIMEOUT
                    )
                )
                .actionGet();
        }
    };

    private static ResourceLoader<CsvTestsDataLoader.ViewConfig> views = new ResourceLoader<>() {
        @Override
        protected void load(CsvTestsDataLoader.ViewConfig view) {
            logger.info("Loading view [{}]", view.name());
            assertAcked(
                cluster.client()
                    .execute(
                        PutViewAction.INSTANCE,
                        new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(view.name(), view.loadQuery()))
                    )
            );
        }

        @Override
        protected void unload(String name) {
            logger.info("Unloading view [{}]", name);
            assertAcked(
                cluster.client()
                    .execute(
                        DeleteViewAction.INSTANCE,
                        new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
                    )
            );
        }
    };

    private abstract static class ResourceLoader<T> {
        private final Set<String> loaded = new HashSet<>();
        private Throwable failure = null;

        protected abstract void load(T resource) throws Throwable;

        protected void unload(String name) {
            throw new UnsupportedOperationException("Unloading is not supported");
        }

        public void maybeLoad(String name, T resource) {
            assertNotNull("Resource [" + name + "] is not found!", resource);
            if (failure == null && loaded.add(name)) {
                try {
                    load(resource);
                } catch (Throwable t) {
                    failure = t;
                    throw new RuntimeException("Resource loading failure", failure);
                }
            }
        }

        public void unloadAll() {
            for (String name : loaded) {
                unload(name);
            }
            loaded.clear();
        }

        public void ensureNoFailures() {
            if (failure != null) {
                throw new RuntimeException("Resource loading failure", failure);
            }
        }
    }

    private static void createCustomRegexConfig(Path configDir) throws IOException {
        // create a subdir for the user-agent with custom regex files so we can test the USER_AGENT with the regex_file option
        Path userAgentDir = configDir.resolve("user-agent");
        Files.createDirectories(userAgentDir);
        try (InputStream is = CsvIT.class.getResourceAsStream("/custom-regexes.yml")) {
            assert is != null : "custom-regexes.yml not found on classpath";
            Files.copy(is, userAgentDir.resolve("custom-regexes.yml"));
        }
    }

    private static void createGeoIpConfig(Path configDir) throws IOException {
        Path geoIpDir = configDir.resolve("ingest-geoip");
        Files.createDirectories(geoIpDir);
        GeoIpTestUtils.copyDefaultDatabases(geoIpDir);
    }

    private static class ResponseListener extends PlainActionFuture<EsqlQueryResponse> {
        private final ThreadPool threadPool;
        private List<String> warnings;

        ResponseListener(ThreadPool threadPool) {
            this.threadPool = threadPool;
        }

        @Override
        public void onResponse(EsqlQueryResponse result) {
            result.mustIncRef();
            warnings = threadPool.getThreadContext().getResponseHeaders().getOrDefault("Warning", List.of());
            if (set(result) == false) {
                result.decRef();
            }
        }
    }
}
