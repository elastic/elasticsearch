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
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.IndexFieldCapabilitiesBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsAction;
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.queryClusterSettings;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLogTests.mockLogFieldProvider;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Tests for the retry logic in {@link EsqlSession}'s {@code analyzeWithRetry}.
 *
 * <p>When {@link org.elasticsearch.xpack.esql.session.FieldNameUtils#resolveFieldNames} returns
 * specific field names (an optimization over requesting ALL_FIELDS), a transient race in
 * cluster-state propagation can cause {@code field_caps} to return incomplete field data for a
 * freshly-created index. This makes the analyzer believe required columns are absent, yielding a
 * {@link VerificationException}. The fix retries the resolution with ALL_FIELDS so that the
 * mapping, which has since propagated, is found on the second attempt.
 */
public class AnalyzeWithRetryTests extends ESTestCase {

    private static final TransportActionServices MOCK_SERVICES = createTransportActionServices();

    /**
     * Reproduces the race condition from issue #150849. The first {@code field_caps} call uses
     * specific field names (the normal optimization path) but returns no {@code date} field,
     * simulating a node that has not yet applied the latest cluster-state mapping update.
     * The second call uses ALL_FIELDS ({@code *}) and returns the full mapping.
     *
     * <p>Without the fix {@code analyzeWithRetry} propagates the {@link VerificationException}
     * immediately when {@code requestFilter == null}. With the fix it retries using ALL_FIELDS
     * and the query succeeds.
     */
    public void testRetryWithAllFieldsWhenSpecificFieldNamesReturnIncompleteData() throws Exception {
        String[] indices = new String[] { "dates" };
        AtomicBoolean planRunnerReached = new AtomicBoolean(false);

        Client esqlClient = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            FieldCapabilitiesRequest fcRequest = (FieldCapabilitiesRequest) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];

            boolean isAllFieldsRequest = Arrays.asList(fcRequest.fields()).contains("*");
            if (isAllFieldsRequest) {
                // ALL_FIELDS retry: return the 'date' field that was missing before.
                listener.onResponse(
                    new EsqlResolveFieldsResponse(new FieldCapabilitiesResponse(indexFieldCapsWithDate(indices), List.of()))
                );
            } else {
                // Specific-fields request: simulate race condition — 'date' mapping not yet
                // visible on this node, so field_caps returns no fields for the index.
                listener.onResponse(new EsqlResolveFieldsResponse(new FieldCapabilitiesResponse(emptyIndexFieldCaps(indices), List.of())));
            }
            return null;
        }).when(esqlClient).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver indexResolver = new IndexResolver(esqlClient);
        List<DataSourcePlugin> plugins = List.of(new DataSourcePlugin() {});
        try (
            DataSourceModule dataSourceModule = new DataSourceModule(
                plugins,
                DataSourceCapabilities.build(plugins),
                Settings.EMPTY,
                blockFactory(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new DataSourceCredentials()
            )
        ) {
            PlanExecutor planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            EsqlQueryRequest request = new EsqlQueryRequest();
            // This is the exact query from BucketColumnMetadataIT.testRenameBucketColumnHasNoMetadata.
            // FieldNameUtils.resolveFieldNames computes {"date", "date.*", "_index"} for this query,
            // triggering the specific-fields optimization that exposes the race condition.
            request.query("""
                FROM dates
                | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
                | RENAME bucket AS bucket_renamed
                """);
            request.allowPartialResults(false);

            EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
            EsqlSession.PlanRunner runPhase = (plan, configuration, foldContext, planTimeProfile, resultListener) -> {
                planRunnerReached.set(true);
                executionInfo.markEndQuery();
                resultListener.onResponse(new Result(List.of(), List.of(), null, configuration, DriverCompletionInfo.EMPTY, executionInfo));
            };

            IndicesExpressionGrouper grouper = (indicesOptions, indexExpressions, returnLocalAll) -> Map.of(
                "",
                new OriginalIndices(indices, IndicesOptions.DEFAULT)
            );

            try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
                planExecutor.esql(
                    request,
                    randomAlphaOfLength(10),
                    TransportVersion.current(),
                    queryClusterSettings(),
                    mockEnrichResolver(),
                    viewService.getViewResolver(),
                    executionInfo,
                    grouper,
                    runPhase,
                    MOCK_SERVICES,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Versioned<Result> result) {
                            // expected after retry succeeds
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail("Query should have succeeded after retry with ALL_FIELDS, but got: " + e.getMessage());
                        }
                    }
                );
            }

            assertTrue("Plan runner should have been called after successful ALL_FIELDS retry", planRunnerReached.get());
        }
    }

    /**
     * Verifies that a genuine unknown-column error (the field is absent even when ALL_FIELDS is
     * used) is still propagated to the caller and not silently swallowed by the retry path.
     */
    public void testGenuineUnknownColumnIsStillReportedAfterRetry() throws Exception {
        String[] indices = new String[] { "dates" };

        Client esqlClient = mock(Client.class);
        // Both specific-fields and ALL_FIELDS calls return empty field caps — the 'date' column
        // genuinely does not exist.
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            listener.onResponse(new EsqlResolveFieldsResponse(new FieldCapabilitiesResponse(emptyIndexFieldCaps(indices), List.of())));
            return null;
        }).when(esqlClient).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());

        IndexResolver indexResolver = new IndexResolver(esqlClient);
        List<DataSourcePlugin> plugins = List.of(new DataSourcePlugin() {});
        try (
            DataSourceModule dataSourceModule = new DataSourceModule(
                plugins,
                DataSourceCapabilities.build(plugins),
                Settings.EMPTY,
                blockFactory(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new DataSourceCredentials()
            )
        ) {
            PlanExecutor planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("""
                FROM dates
                | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
                | RENAME bucket AS bucket_renamed
                """);
            request.allowPartialResults(false);

            EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
            EsqlSession.PlanRunner runPhase = (plan, configuration, foldContext, planTimeProfile, r) -> fail(
                "plan runner should not be reached for a genuinely missing column"
            );

            IndicesExpressionGrouper grouper = (indicesOptions, indexExpressions, returnLocalAll) -> Map.of(
                "",
                new OriginalIndices(indices, IndicesOptions.DEFAULT)
            );

            AtomicBoolean failureCaptured = new AtomicBoolean(false);
            try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
                planExecutor.esql(
                    request,
                    randomAlphaOfLength(10),
                    TransportVersion.current(),
                    queryClusterSettings(),
                    mockEnrichResolver(),
                    viewService.getViewResolver(),
                    executionInfo,
                    grouper,
                    runPhase,
                    MOCK_SERVICES,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Versioned<Result> result) {
                            fail("expected VerificationException, not success");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(e, instanceOf(VerificationException.class));
                            assertThat(e.getMessage(), containsString("Unknown column [date]"));
                            failureCaptured.set(true);
                        }
                    }
                );
            }

            assertTrue("Expected VerificationException to be reported", failureCaptured.get());
        }
    }

    // --- helpers ---

    private List<FieldCapabilitiesIndexResponse> indexFieldCapsWithDate(String[] indices) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (String idx : indices) {
            responses.add(
                new FieldCapabilitiesIndexResponse(
                    idx,
                    idx,
                    Map.of("date", new IndexFieldCapabilitiesBuilder("date", "date").build()),
                    true,
                    IndexMode.STANDARD
                )
            );
        }
        return responses;
    }

    private List<FieldCapabilitiesIndexResponse> emptyIndexFieldCaps(String[] indices) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (String idx : indices) {
            responses.add(new FieldCapabilitiesIndexResponse(idx, idx, Map.of(), true, IndexMode.STANDARD));
        }
        return responses;
    }

    @SuppressWarnings("unchecked")
    private EnrichPolicyResolver mockEnrichResolver() {
        EnrichPolicyResolver enrichResolver = mock(EnrichPolicyResolver.class);
        doAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            ActionListener<EnrichResolution> listener = (ActionListener<EnrichResolution>) arguments[arguments.length - 1];
            listener.onResponse(new EnrichResolution());
            return null;
        }).when(enrichResolver).resolvePolicies(any(), any(), any(), any());
        return enrichResolver;
    }

    private EsqlQueryLog mockQueryLog() {
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(
                Arrays.asList(
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_WARN_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_INFO_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_DEBUG_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_THRESHOLD_TRACE_SETTING,
                    EsqlPlugin.ESQL_QUERYLOG_INCLUDE_USER_SETTING
                )
            )
        );
        return new EsqlQueryLog(clusterSettings, mockLogFieldProvider());
    }

    private PlanExecutor buildPlanExecutor(IndexResolver indexResolver, DataSourceModule dataSourceModule) {
        return new PlanExecutor(
            indexResolver,
            MeterRegistry.NOOP,
            new XPackLicenseState(() -> 0L),
            mockQueryLog(),
            List.of(),
            CrossProjectModeDecider.NOOP,
            dataSourceModule,
            TEST_FUNCTION_REGISTRY,
            PromqlFunctionRegistry.INSTANCE,
            TEST_PARSER,
            null
        );
    }

    private BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    private static TransportActionServices createTransportActionServices() {
        ClusterService clusterService = createMockClusterService();
        return new TransportActionServices(
            createMockTransportService(),
            mock(SearchService.class),
            null,
            clusterService,
            mock(ProjectResolver.class),
            mock(IndexNameExpressionResolver.class),
            null,
            new InferenceService(mock(Client.class), clusterService),
            UserAgentParserRegistry.NOOP,
            new BlockFactoryProvider(PlannerUtils.NON_BREAKING_BLOCK_FACTORY),
            new PlannerSettings.Holder(clusterService),
            CrossProjectModeDecider.NOOP
        );
    }

    private static ClusterService createMockClusterService() {
        ClusterService service = mock(ClusterService.class);
        doReturn(new ClusterName("test-cluster")).when(service).getClusterName();
        doReturn(Settings.EMPTY).when(service).getSettings();
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(InferenceSettings.getSettings());
        settings.addAll(PlannerSettings.settings());
        doReturn(new ClusterSettings(Settings.EMPTY, settings)).when(service).getClusterSettings();
        return service;
    }

    private static TransportService createMockTransportService() {
        TransportService service = mock(TransportService.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        doReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE).when(threadPool).executor(anyString());
        doReturn(threadPool).when(service).getThreadPool();
        return service;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
