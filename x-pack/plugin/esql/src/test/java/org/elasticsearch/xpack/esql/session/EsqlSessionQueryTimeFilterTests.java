/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilities;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.FederationLicense;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.queryClusterSettings;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLogTests.mockLogFieldProvider;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * How {@link EsqlSession} uses the time range that a query restricts {@code @timestamp} to when it resolves the indices:
 * which index filter each field caps request carries, and when the indices are resolved again with less filtering.
 * <p>
 * Queries run through the session up to the analyzed plan. Field caps is the one thing that is not real: the client that
 * {@link IndexResolver} sends its requests to records the index filter of each request, and answers as if two indices matched
 * the pattern. {@code logs-recent} holds data in any time range. {@code logs-old} holds none in the time ranges that the tests
 * query, so a request whose filter bounds {@code @timestamp} does not see it. Each index maps a field of its own, which tells
 * the indices that a query was analyzed with by the columns of its plan.
 * <p>
 * The mocks stand for the services that a session takes but that analysis does not use: the cluster service, the transport
 * service and its thread pool (everything runs on the calling thread), the enrich policies, of which there are none.
 */
public class EsqlSessionQueryTimeFilterTests extends ESTestCase {

    private static final long T1 = Instant.parse("2024-05-01T00:00:00Z").toEpochMilli();
    private static final String RECENT = "logs-recent";
    private static final String OLD = "logs-old";
    private static final String TIME_RANGE = " | WHERE @timestamp >= \"2024-05-01T00:00:00Z\"";
    private static final QueryBuilder TIME_FILTER = new RangeQueryBuilder("@timestamp").format("epoch_millis").gte(T1);
    private static final QueryBuilder REQUEST_FILTER = QueryBuilders.termQuery("message", "error");

    /** The index filter of every field caps request that the query under test sent, in order. */
    private final List<QueryBuilder> indexFilters = Collections.synchronizedList(new ArrayList<>());
    /** The indices that field caps answers a request with, given its index filter. */
    private Function<QueryBuilder, List<String>> matchingIndices;
    private InMemoryViewService viewService;
    /** Field caps answers on the thread pool that it answers on in a node, which index resolution checks. */
    private ThreadPool fieldCapsThreadPool;
    private DataSourceModule dataSourceModule;

    @Before
    public void setup() {
        matchingIndices = filter -> boundsTimestamp(filter) ? List.of(RECENT) : List.of(RECENT, OLD);
        fieldCapsThreadPool = new TestThreadPool(getTestName());
        viewService = InMemoryViewService.makeViewService();
        viewService.addIndex(ProjectId.DEFAULT, RECENT);
        viewService.addIndex(ProjectId.DEFAULT, OLD);
        List<DataSourcePlugin> plugins = List.of(new DataSourcePlugin() {});
        dataSourceModule = new DataSourceModule(
            plugins,
            DataSourceCapabilities.build(plugins),
            Settings.EMPTY,
            BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(NoopCircuitBreaker.INSTANCE).build(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(mock(EncryptionService.class)),
            () -> false
        );
    }

    @After
    public void cleanup() throws Exception {
        dataSourceModule.close();
        viewService.close();
        terminate(fieldCapsThreadPool);
    }

    public void testQueryWithoutTimeRange() {
        assertThat(columns(analyze("FROM logs-* | KEEP message", null)), contains("message"));
        assertThat(indexFilters, contains(nullValue()));
    }

    public void testTimeRangeOfQueryFiltersIndexResolution() {
        List<String> columns = columns(analyze("FROM logs-*" + TIME_RANGE, null));
        assertThat(indexFilters, contains(TIME_FILTER));
        // The index without data in the time range is not part of the resolution, and neither are its fields.
        assertThat(columns, hasItem("recent_only"));
        assertThat(columns, not(hasItem("old_only")));
    }

    /** A request filter that says nothing about time is narrowed by the time range of the query. */
    public void testTimeRangeOfQueryNarrowsRequestFilter() {
        analyze("FROM logs-*" + TIME_RANGE, REQUEST_FILTER);
        assertThat(indexFilters, contains(new BoolQueryBuilder().filter(REQUEST_FILTER).filter(TIME_FILTER)));
    }

    /**
     * A request filter that has a time range of its own is the only one to filter index resolution, whatever the time range
     * of the query is, and whatever else the request filter holds.
     */
    public void testTimeRangeOfQueryNotUsedWhenRequestFilterHasOne() {
        QueryBuilder requestRange = QueryBuilders.rangeQuery("@timestamp").gte("2024-04-01T00:00:00Z").lte("2024-06-01T00:00:00Z");
        analyze("FROM logs-*" + TIME_RANGE, requestRange);
        assertThat(indexFilters, contains(requestRange));

        indexFilters.clear();
        QueryBuilder oneSided = QueryBuilders.rangeQuery("@timestamp").gte("now-1d");
        analyze("FROM logs-*" + TIME_RANGE, oneSided);
        assertThat(indexFilters, contains(oneSided));

        indexFilters.clear();
        QueryBuilder nested = new BoolQueryBuilder().filter(REQUEST_FILTER).filter(requestRange);
        analyze("FROM logs-*" + TIME_RANGE, nested);
        assertThat(indexFilters, contains(nested));
    }

    /** With a time range in the request filter, the query is tried again without that filter only, as it was before. */
    public void testRetryWhenRequestFilterHasTimeRange() {
        QueryBuilder requestRange = QueryBuilders.rangeQuery("@timestamp").gte("2024-04-01T00:00:00Z");
        assertThat(columns(analyze("FROM logs-*" + TIME_RANGE + " | KEEP old_only", requestRange)), contains("old_only"));
        assertThat(indexFilters, contains(requestRange, null));
    }

    /** A filter that the query applies after a LIMIT is not one on the rows of the index. */
    public void testTimeRangeThatDoesNotApplyToTheIndex() {
        List<String> columns = columns(analyze("FROM logs-* | LIMIT 10" + TIME_RANGE, null));
        assertThat(indexFilters, contains(nullValue()));
        assertThat(columns, hasItem("old_only"));
    }

    /**
     * With no data in the time range field caps returns no index at all. The indices are resolved again without the time
     * range, so that the query gets the columns of the indices that match its pattern.
     */
    public void testRetryWhenTimeRangeLeavesNoIndex() {
        matchingIndices = filter -> boundsTimestamp(filter) ? List.of() : List.of(RECENT, OLD);
        List<String> columns = columns(analyze("FROM logs-*" + TIME_RANGE, null));
        assertThat(indexFilters, contains(TIME_FILTER, null));
        assertThat(columns, hasItem("recent_only"));
        assertThat(columns, hasItem("old_only"));
    }

    /** The request filter stays when the time range of the query is given up on. */
    public void testRetryWhenTimeRangeLeavesNoIndexKeepsRequestFilter() {
        matchingIndices = filter -> boundsTimestamp(filter) ? List.of() : List.of(RECENT, OLD);
        analyze("FROM logs-*" + TIME_RANGE, REQUEST_FILTER);
        assertThat(indexFilters, contains(new BoolQueryBuilder().filter(REQUEST_FILTER).filter(TIME_FILTER), REQUEST_FILTER));
    }

    /** A field that only the pruned index maps fails the analysis, which is tried again without the time range. */
    public void testRetryWhenAnalysisFails() {
        assertThat(columns(analyze("FROM logs-*" + TIME_RANGE + " | KEEP old_only", null)), contains("old_only"));
        assertThat(indexFilters, contains(TIME_FILTER, null));
    }

    /**
     * With a request filter as well, the time range of the query goes first and the request filter second, as it did before
     * queries had a time range to go by.
     */
    public void testRetryInStagesWhenAnalysisFails() {
        matchingIndices = filter -> filter == null ? List.of(RECENT, OLD) : List.of(RECENT);
        assertThat(columns(analyze("FROM logs-*" + TIME_RANGE + " | KEEP old_only", REQUEST_FILTER)), contains("old_only"));
        assertThat(indexFilters, contains(new BoolQueryBuilder().filter(REQUEST_FILTER).filter(TIME_FILTER), REQUEST_FILTER, null));
    }

    public void testInvalidQueryFailsAfterRetry() {
        Exception failure = expectThrows(
            VerificationException.class,
            () -> analyze("FROM logs-*" + TIME_RANGE + " | KEEP no_such_field", null)
        );
        assertThat(failure.getMessage(), org.hamcrest.Matchers.containsString("no_such_field"));
        assertThat(indexFilters, contains(TIME_FILTER, null));
    }

    /**
     * An index that holds {@code @timestamp} in {@code _source} without mapping it never matches the filter, while its rows
     * pass a condition on the value once it is loaded. The time range is not used then, for the query or for a view of it.
     */
    public void testTimeRangeNotUsedWhenUnmappedFieldsAreLoaded() {
        for (String mode : new String[] { "load", "LOAD_ALL" }) {
            indexFilters.clear();
            analyze("SET unmapped_fields=\"" + mode + "\"; FROM logs-*" + TIME_RANGE, null);
            assertThat(mode, indexFilters, contains(nullValue()));
        }
        addView("loading_view", "SET unmapped_fields=\"load\"; FROM logs-* | KEEP @timestamp, message");
        indexFilters.clear();
        analyze("FROM loading_view" + TIME_RANGE, null);
        assertThat(indexFilters, contains(nullValue()));
    }

    /** A nullified {@code @timestamp} is null, which no condition on it accepts, just like the filter. */
    public void testTimeRangeUsedWhenUnmappedFieldsAreNullified() {
        List<String> columns = columns(analyze("SET unmapped_fields=\"nullify\"; FROM logs-*" + TIME_RANGE, null));
        assertThat(indexFilters, contains(TIME_FILTER));
        assertThat(columns, not(hasItem("old_only")));
    }

    /** Nothing fails the analysis of nullified fields, the indices are resolved again all the same when none is left. */
    public void testRetryWhenTimeRangeLeavesNoIndexWithNullifiedFields() {
        matchingIndices = filter -> boundsTimestamp(filter) ? List.of() : List.of(RECENT, OLD);
        List<String> columns = columns(analyze("SET unmapped_fields=\"nullify\"; FROM logs-*" + TIME_RANGE, null));
        assertThat(indexFilters, contains(TIME_FILTER, null));
        assertThat(columns, hasItem("old_only"));
    }

    /**
     * The request filter applies to the output of a view and does not filter the indices that the view reads. The time range
     * of the query does, when the view hands {@code @timestamp} on.
     */
    public void testView() {
        addView("logs_view", "FROM logs-* | DROP recent_only");
        analyze("FROM logs_view" + TIME_RANGE, REQUEST_FILTER);
        assertThat(indexFilters, contains(TIME_FILTER));
    }

    public void testViewThatDoesNotHandTimestampOn() {
        addView("logs_counts", "FROM logs-* | STATS c = COUNT(*) BY @timestamp");
        analyze("FROM logs_counts" + TIME_RANGE, null);
        assertThat(indexFilters, contains(nullValue()));
    }

    /**
     * A PromQL query is analyzed with its unmapped fields nullified, so a pattern without indices does not fail its analysis.
     * The indices are resolved again without the time range all the same. Both requests carry the filter on the index mode
     * that a time series source adds.
     */
    public void testPromqlRetryWhenTimeRangeLeavesNoIndex() {
        matchingIndices = filter -> boundsTimestamp(filter) ? List.of() : List.of(RECENT, OLD);
        QueryBuilder timeSeries = QueryBuilders.termQuery("_index_mode", "time_series");
        QueryBuilder promqlRange = new RangeQueryBuilder("@timestamp").format("epoch_millis")
            .gte(T1 - TimeValue.timeValueMinutes(5).millis())
            .lt(T1 + TimeValue.timeValueHours(1).millis() + 1);
        try {
            analyze("PROMQL index=logs-* step=1m start=\"2024-05-01T00:00:00Z\" end=\"2024-05-01T01:00:00Z\" v=(avg(message))", null);
        } catch (Exception e) {
            // The indices of the test are not time series indices. What the query comes to does not matter here.
            logger.info("PromQL query failed", e);
        }
        assertThat(indexFilters, contains(new BoolQueryBuilder().filter(promqlRange).filter(timeSeries), timeSeries));
    }

    /**
     * A time series source is resolved with the time range of the query next to the filter on the index mode that it adds.
     * A query for exemplars is no different: its exemplar data streams follow from the indices that the pattern resolves to.
     */
    public void testTimeSeriesSources() {
        QueryBuilder timeSeries = QueryBuilders.termQuery("_index_mode", "time_series");
        for (String settings : new String[] { "", "SET exemplars=true; " }) {
            indexFilters.clear();
            try {
                analyze(settings + "TS logs-*" + TIME_RANGE + " | STATS m = MAX(MAX_OVER_TIME(value))", null);
            } catch (Exception e) {
                // The indices of the test are not time series indices. What the query comes to does not matter here.
                logger.info("time series query failed", e);
            }
            assertThat(settings, indexFilters.get(0), equalTo(new BoolQueryBuilder().filter(TIME_FILTER).filter(timeSeries)));
        }
    }

    private static boolean boundsTimestamp(QueryBuilder filter) {
        if (filter instanceof BoolQueryBuilder bool) {
            for (QueryBuilder clause : bool.filter()) {
                if (boundsTimestamp(clause)) {
                    return true;
                }
            }
        }
        return filter instanceof RangeQueryBuilder range && range.fieldName().equals("@timestamp");
    }

    private static List<String> columns(List<Attribute> output) {
        List<String> names = new ArrayList<>();
        for (Attribute attribute : output) {
            names.add(attribute.name());
        }
        return names;
    }

    private void addView(String name, String query) {
        PlainActionFuture<Object> future = new PlainActionFuture<>();
        viewService.putView(
            ProjectId.DEFAULT,
            new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query, null)),
            future.map(response -> response)
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
    }

    /**
     * Runs the query through the session until it hands the plan over for execution, returning the columns that the query was
     * analyzed to, or throwing what it failed with.
     */
    private List<Attribute> analyze(String query, QueryBuilder requestFilter) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        // An explicit limit, for the query not to warn about the default one that it would get.
        request.query(query + " | LIMIT 10");
        request.filter(requestFilter);
        request.allowPartialResults(false);
        // Nothing is executed: reaching the runner means that the query was analyzed and planned.
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
        AtomicReference<List<Attribute>> output = new AtomicReference<>();
        EsqlSession.PlanRunner runner = (role, plan, configuration, foldContext, planTimeProfile, listener) -> {
            output.set(plan.output());
            executionInfo.markEndQuery();
            listener.onResponse(new Result(plan.output(), List.of(), null, configuration, DriverCompletionInfo.EMPTY, executionInfo, null));
        };
        IndicesExpressionGrouper localIndices = (indicesOptions, indexExpressions, returnLocalAll) -> Map.of(
            "",
            new OriginalIndices(indexExpressions, IndicesOptions.DEFAULT)
        );
        PlainActionFuture<Versioned<Result>> future = new PlainActionFuture<>();
        planExecutor().esql(
            request,
            randomAlphaOfLength(10),
            TransportVersion.current(),
            queryClusterSettings(),
            noEnrichPolicies(),
            viewService.getViewResolver(),
            new DatasetResolver(null, null, CrossProjectModeDecider.NOOP, true, new FederationLicense(() -> null)),
            executionInfo,
            localIndices,
            runner,
            transportActionServices(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            1,
            () -> false,
            future
        );
        future.actionGet(TEST_REQUEST_TIMEOUT);
        return output.get();
    }

    private PlanExecutor planExecutor() {
        return new PlanExecutor(
            new IndexResolver(fieldCapsClient(), () -> true),
            MeterRegistry.NOOP,
            new XPackLicenseState(() -> 0L),
            new EsqlQueryLog(
                new ClusterSettings(
                    Settings.EMPTY,
                    Set.of(
                        EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
                        EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
                        EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
                        EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
                        EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING
                    )
                ),
                mockLogFieldProvider()
            ),
            List.of(),
            CrossProjectModeDecider.NOOP,
            dataSourceModule,
            TEST_FUNCTION_REGISTRY,
            PromqlFunctionRegistry.INSTANCE,
            TEST_PARSER,
            null,
            EsqlTestUtils.TEST_ANALYSIS_REGISTRY
        );
    }

    /**
     * The client that index resolution sends its field caps requests to. It records the index filter of the request and
     * answers with the mappings of the indices that {@link #matchingIndices} lets through.
     */
    private Client fieldCapsClient() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            EsqlResolveFieldsRequest request = invocation.getArgument(1);
            ActionListener<EsqlResolveFieldsResponse> listener = invocation.getArgument(2);
            QueryBuilder indexFilter = request.fieldCapsRequest().indexFilter();
            indexFilters.add(indexFilter);
            List<FieldCapabilitiesIndexResponse> indices = new ArrayList<>();
            for (String index : matchingIndices.apply(indexFilter)) {
                indices.add(new FieldCapabilitiesIndexResponse(index, index, mapping(index), true, IndexMode.STANDARD, 0, 0, 0, 0));
            }
            EsqlResolveFieldsResponse response = new EsqlResolveFieldsResponse(
                FieldCapabilitiesResponse.builder()
                    .withIndexResponses(indices)
                    .withResolvedLocally(new ResolvedIndexExpressions(List.of(), null))
                    .build()
            );
            fieldCapsThreadPool.executor(ThreadPool.Names.SEARCH_COORDINATION).execute(() -> listener.onResponse(response));
            return null;
        }).when(client).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());
        return client;
    }

    private static Map<String, IndexFieldCapabilities> mapping(String index) {
        Map<String, IndexFieldCapabilities> fields = new HashMap<>();
        fields.put("@timestamp", new IndexFieldCapabilitiesBuilder("@timestamp", "date").build());
        fields.put("message", new IndexFieldCapabilitiesBuilder("message", "keyword").build());
        String own = index.equals(RECENT) ? "recent_only" : "old_only";
        fields.put(own, new IndexFieldCapabilitiesBuilder(own, "keyword").build());
        return fields;
    }

    private static EnrichPolicyResolver noEnrichPolicies() {
        EnrichPolicyResolver resolver = mock(EnrichPolicyResolver.class);
        doAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            @SuppressWarnings("unchecked")
            ActionListener<EnrichResolution> listener = (ActionListener<EnrichResolution>) arguments[arguments.length - 1];
            listener.onResponse(new EnrichResolution());
            return null;
        }).when(resolver).resolvePolicies(any(), any(), any(), any(), any());
        return resolver;
    }

    private static TransportActionServices transportActionServices() {
        ClusterService clusterService = mock(ClusterService.class);
        doReturn(new ClusterName("test-cluster")).when(clusterService).getClusterName();
        doReturn(Settings.EMPTY).when(clusterService).getSettings();
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(InferenceSettings.getSettings());
        settings.addAll(PlannerSettings.settings());
        settings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        doReturn(new ClusterSettings(Settings.EMPTY, settings)).when(clusterService).getClusterSettings();
        // Query-setting resolution reads the cluster-state settings for operator-supplied defaults.
        doReturn(ClusterState.EMPTY_STATE).when(clusterService).state();

        ThreadPool threadPool = mock(ThreadPool.class);
        doReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE).when(threadPool).executor(anyString());
        TransportService transportService = mock(TransportService.class);
        doReturn(threadPool).when(transportService).getThreadPool();

        return new TransportActionServices(
            transportService,
            mock(SearchService.class),
            null,
            clusterService,
            mock(ProjectResolver.class),
            mock(IndexNameExpressionResolver.class),
            new UsageService(),
            new InferenceService(mock(Client.class), clusterService),
            UserAgentParserRegistry.NOOP,
            IpLocationService.NOOP,
            new BlockFactoryProvider(PlannerUtils.NON_BREAKING_BLOCK_FACTORY),
            new PlannerSettings.Holder(clusterService),
            CrossProjectModeDecider.NOOP
        );
    }
}
