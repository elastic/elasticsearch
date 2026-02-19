/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.enrich.EnrichPlugin;
import org.elasticsearch.xpack.esql.CsvTestUtils.ActualResults;
import org.elasticsearch.xpack.esql.CsvTestUtils.ExpectedResults;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramMapperPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.elasticsearch.xpack.versionfield.VersionFieldPlugin;
import org.elasticsearch.xpack.wildcard.Wildcard;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URL;
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
import static org.elasticsearch.xpack.esql.CsvAssert.assertResultsWithTransformer;
import static org.elasticsearch.xpack.esql.CsvSpecReader.specParser;
import static org.elasticsearch.xpack.esql.CsvTestUtils.isEnabled;
import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.classpathResources;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class CsvIT extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(CsvIT.class);

    private static InternalTestCluster cluster;
    private static String currentGroupName = null;

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
        return SpecReader.readScriptSpec(urls, specParser());
    }

    @BeforeClass
    public static void setupCluster() throws Exception {
        long start = System.currentTimeMillis();
        logger.info("Creating test cluster");
        cluster = new InternalTestCluster(
            randomLong(),
            createTempDir(),
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
                        .build();
                }

                @Override
                public java.nio.file.Path nodeConfigPath(int nodeOrdinal) {
                    return null;
                }
            },
            0,
            "node_",
            List.of(
                getTestTransportPlugin(),
                EsqlTestPlugin.class,
                AggregateMetricMapperPlugin.class,
                AnalyticsPlugin.class,
                ConstantKeywordMapperPlugin.class,
                EnrichPlugin.class,
                IngestCommonPlugin.class,
                ExponentialHistogramMapperPlugin.class,
                LocalStateInferencePlugin.class,
                MapperExtrasPlugin.class,
                SpatialPlugin.class,
                UnsignedLongMapperPlugin.class,
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
        assumeTrue("Test " + testName + " is not enabled", isEnabled(testName, instructions, Version.CURRENT));

        currentGroupName = groupName;
        // verify no prior failures
        indices.ensureNoFailures();
        enrich.ensureNoFailures();
        views.ensureNoFailures();

        // skipUnsupportedCapability(EsqlCapabilities.Cap.SEMANTIC_TEXT_FIELD_CAPS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.TEXT_EMBEDDING_FUNCTION);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_V6);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_OPTIONS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.CATEGORIZE_MULTIPLE_GROUPINGS);
        skipUnsupportedCapability(EsqlCapabilities.Cap.RERANK);
        skipUnsupportedCapability(EsqlCapabilities.Cap.COMPLETION);
        // runs in a single cluster/single node mode
        skipUnsupportedCapability(EsqlCapabilities.Cap.METADATA_FIELDS_REMOTE_TEST);

        assumeFalse(
            "CSV tests cannot handle EXTERNAL sources (requires QA integration tests)",
            testCase.query.trim().toUpperCase(java.util.Locale.ROOT).startsWith("EXTERNAL")
        );

        var request = syncEsqlQueryRequest(testCase.query);
        var listener = new ResponseListener(cluster.getInstance(TransportService.class).getThreadPool());
        cluster.client().execute(EsqlQueryAction.INSTANCE, request, listener);
        try (var response = listener.actionGet(30, TimeUnit.SECONDS)) {
            ExpectedResults expected = loadCsvSpecValues(testCase.expectedResults);
            ActualResults actual = new ActualResults(
                response.zoneId(),
                response.columns().stream().map(ColumnInfoImpl::name).toList(),
                response.columns().stream().map(column -> CsvTestUtils.Type.asType(column.type().nameUpper())).toList(),
                response.columns().stream().map(ColumnInfoImpl::type).toList(),
                response.pages(),
                Map.of()
            );

            assertResultsWithTransformer(
                expected,
                actual.columnNames(),
                actual.columnTypes(),
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

    private void skipUnsupportedCapability(EsqlCapabilities.Cap capability) {
        assumeFalse("Skipping unsupported capability: " + capability, testCase.requiredCapabilities.contains(capability.capabilityName()));
    }

    public boolean logResults() {
        return false;
    }

    public static class EsqlTestPlugin extends EsqlPlugin implements NetworkPlugin {
        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null));
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
    }

    private static void loadViews() {
        // TODO We should instead load views once and never unload them
        if ("views".equals(currentGroupName)) {
            CsvTestsDataLoader.VIEW_CONFIGS.forEach(view -> views.maybeLoad(view));
        } else {
            views.unloadAll();
        }
    }

    private static void loadIndices(FieldCapabilitiesRequest request) {
        Stream.of(request.indices()).flatMap(pattern -> {
            assert pattern.contains("<") == false : "Date-math is not supported in test";
            if (pattern.contains("*")) {
                assert pattern.endsWith("*") : "Only suffix patterns are supported in test";
                var prefix = pattern.substring(pattern.startsWith("-") ? 1 : 0, pattern.length() - 1);
                return CsvTestsDataLoader.CSV_DATASET_MAP.values().stream().filter(ds -> ds.indexName().startsWith(prefix));
            } else {
                return Stream.of(CsvTestsDataLoader.CSV_DATASET_MAP.get(pattern));
            }
        }).filter(Objects::nonNull).forEach(resource -> indices.maybeLoad(resource));
    }

    private static void loadEnrichPolicy(EnrichPolicyResolver.LookupRequest request) {
        for (var name : request.policyNames) {
            enrich.maybeLoad(
                CsvTestsDataLoader.ENRICH_POLICIES.stream().filter(p -> Objects.equals(p.policyName(), name)).findFirst().get()
            );
        }
    }

    private static ResourceLoader<CsvTestsDataLoader.TestDataset> indices = new ResourceLoader<>() {
        @Override
        protected String name(CsvTestsDataLoader.TestDataset resource) {
            return resource.indexName();
        }

        @Override
        protected void load(CsvTestsDataLoader.TestDataset dataset) throws IOException {
            logger.info("Loading dataset [{}]", dataset.indexName());
            if (dataset.inferenceEndpoint() != null) {
                dataset.inferenceEndpoint()
                    .stream()
                    .map(CsvTestsDataLoader::findInferenceConfigByName)
                    .forEach(c -> inference.maybeLoad(c));
            }
            assertAcked(
                cluster.client()
                    .admin()
                    .indices()
                    .prepareCreate(dataset.indexName())
                    .setMapping(CsvTestsDataLoader.readMappingFile(dataset))
                    .setSettings(dataset.loadSettings())
            );
            if (dataset.dataFileName() != null) {
                var bulk = cluster.client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (var document : CsvTestsDataLoader.readCsvDocuments(dataset.streamData(), dataset.allowSubFields())) {
                    bulk.add(
                        cluster.client()
                            .prepareIndex(dataset.indexName())
                            .setId(document.id())
                            .setSource(document.json().toString(), XContentType.JSON)
                    );
                }
                if (bulk.numberOfActions() > 0) {
                    var result = bulk.get();
                    assertFalse(
                        "Must load dataset [" + dataset.indexName() + "] successfully: " + result.buildFailureMessage(),
                        result.hasFailures()
                    );
                }
            }
        }
    };

    private static ResourceLoader<CsvTestsDataLoader.EnrichConfig> enrich = new ResourceLoader<>() {
        @Override
        protected String name(CsvTestsDataLoader.EnrichConfig resource) {
            return resource.policyName();
        }

        @Override
        protected void load(CsvTestsDataLoader.EnrichConfig policy) throws IOException {
            logger.info("Creating policy [{}]", policy.policyFileName());
            var p = EnrichPolicy.fromXContent(
                JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, policy.streamPolicy())
            );
            for (var index : p.getIndices()) {
                indices.maybeLoad(CsvTestsDataLoader.CSV_DATASET_MAP.get(index));
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
        protected String name(CsvTestsDataLoader.InferenceConfig resource) {
            return resource.name();
        }

        @Override
        protected void load(CsvTestsDataLoader.InferenceConfig inference) {
            logger.info("Loading inference [{}]", inference.name());
            cluster.client()
                .execute(
                    PutInferenceModelAction.INSTANCE,
                    new PutInferenceModelAction.Request(
                        inference.type(),
                        inference.name(),
                        new BytesArray(inference.definition()),
                        XContentType.JSON,
                        TEST_REQUEST_TIMEOUT
                    )
                )
                .actionGet();
        }
    };

    private static ResourceLoader<CsvTestsDataLoader.ViewConfig> views = new ResourceLoader<>() {
        @Override
        protected String name(CsvTestsDataLoader.ViewConfig resource) {
            return resource.name();
        }

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
                    .execute(DeleteViewAction.INSTANCE, new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, name))
            );
        }
    };

    private abstract static class ResourceLoader<T> {
        private final Set<String> loaded = new HashSet<>();
        private Throwable failure = null;

        protected abstract String name(T resource);

        protected abstract void load(T resource) throws Throwable;

        protected void unload(String name) {
            throw new UnsupportedOperationException("Unloading is not supported");
        }

        public void maybeLoad(T resource) {
            var name = name(resource);
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
