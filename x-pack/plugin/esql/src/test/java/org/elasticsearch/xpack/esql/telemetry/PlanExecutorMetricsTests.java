/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
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
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.search.crossproject.ProjectRoutingRequestInfo;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
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
import org.elasticsearch.xpack.esql.action.EsqlResolveFieldsResponse;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.datasources.DataSourceCapabilities;
import org.elasticsearch.xpack.esql.datasources.DataSourceCredentials;
import org.elasticsearch.xpack.esql.datasources.DataSourceModule;
import org.elasticsearch.xpack.esql.datasources.DatasetResolver;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.esql.session.IndexResolver;
import org.elasticsearch.xpack.esql.session.Result;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.queryClusterSettings;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLogTests.mockLogFieldProvider;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class PlanExecutorMetricsTests extends ESTestCase {

    private static final EncryptionService ENCRYPTION_SERVICE = mock(EncryptionService.class);

    private static final TransportActionServices MOCK_TRANSPORT_ACTION_SERVICES = createTransportActionServices(
        new UsageService(),
        CrossProjectModeDecider.NOOP
    );

    private static TransportActionServices createTransportActionServices(UsageService usageService, CrossProjectModeDecider cpsDecider) {
        ClusterService clusterService = createMockClusterService();
        return new TransportActionServices(
            createMockTransportService(),
            mock(SearchService.class),
            null,
            clusterService,
            mock(ProjectResolver.class),
            mock(IndexNameExpressionResolver.class),
            usageService,
            new InferenceService(mock(Client.class), clusterService),
            UserAgentParserRegistry.NOOP,
            IpLocationService.NOOP,
            new BlockFactoryProvider(PlannerUtils.NON_BREAKING_BLOCK_FACTORY),
            new PlannerSettings.Holder(clusterService),
            cpsDecider
        );
    }

    private static ClusterService createMockClusterService() {
        var service = mock(ClusterService.class);
        doReturn(new ClusterName("test-cluster")).when(service).getClusterName();
        doReturn(Settings.EMPTY).when(service).getSettings();

        // Create ClusterSettings with the required inference settings
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(InferenceSettings.getSettings());
        settings.addAll(PlannerSettings.settings());
        var clusterSettings = new ClusterSettings(Settings.EMPTY, settings);
        doReturn(clusterSettings).when(service).getClusterSettings();
        return service;
    }

    private static TransportService createMockTransportService() {
        var service = mock(TransportService.class);
        doReturn(createMockThreadPool()).when(service).getThreadPool();
        return service;
    }

    private static ThreadPool createMockThreadPool() {
        var threadPool = mock(ThreadPool.class);
        doReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE).when(threadPool).executor(anyString());
        return threadPool;
    }

    private static Result createPlanRunnerResult(Configuration configuration, EsqlExecutionInfo executionInfo) {
        executionInfo.markEndQuery();
        return new Result(List.of(), List.of(), null, configuration, DriverCompletionInfo.EMPTY, executionInfo);
    }

    @SuppressWarnings("unchecked")
    EnrichPolicyResolver mockEnrichResolver() {
        EnrichPolicyResolver enrichResolver = mock(EnrichPolicyResolver.class);
        doAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            ActionListener<EnrichResolution> listener = (ActionListener<EnrichResolution>) arguments[arguments.length - 1];
            listener.onResponse(new EnrichResolution());
            return null;
        }).when(enrichResolver).resolvePolicies(any(), any(), any(), any(), any());
        return enrichResolver;
    }

    EsqlQueryLog mockQueryLog() {
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

    public void testFailedMetric() throws Exception {
        IndexResolver indexResolver = mockIndexResolver();

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            var planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);
            var enrichResolver = mockEnrichResolver();

            var request = new EsqlQueryRequest();
            // test a failed query: xyz field doesn't exist
            request.query("from test | stats m = max(xyz)");
            request.allowPartialResults(false);
            EsqlSession.PlanRunner runPhase = (p, configuration, foldContext, planTimeProfile, r) -> fail("this shouldn't happen");
            IndicesExpressionGrouper groupIndicesByCluster = (indicesOptions, indexExpressions, returnLocalAll) -> Map.of(
                "",
                new OriginalIndices(new String[] { "test" }, IndicesOptions.DEFAULT)
            );

            try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
                planExecutor.esql(
                    request,
                    randomAlphaOfLength(10),
                    TransportVersion.current(),
                    queryClusterSettings(),
                    enrichResolver,
                    viewService.getViewResolver(),
                    noDatasetsResolver(),
                    createEsqlExecutionInfo(randomBoolean()),
                    groupIndicesByCluster,
                    runPhase,
                    MOCK_TRANSPORT_ACTION_SERVICES,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    1,
                    () -> false,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Versioned<Result> result) {
                            fail("this shouldn't happen");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assertThat(e, instanceOf(VerificationException.class));
                        }
                    }
                );
            }

            // check we recorded the failure and that the query actually came
            assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
            assertEquals(1, planExecutor.metrics().stats().get("queries._all.total"));
            assertEquals(0, planExecutor.metrics().stats().get("features.stats"));

            // fix the failing query: foo field does exist
            request.query("from test | stats m = max(foo)");
            var successExecutionInfo = createEsqlExecutionInfo(randomBoolean());
            runPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, successExecutionInfo)
            );
            try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
                planExecutor.esql(
                    request,
                    randomAlphaOfLength(10),
                    TransportVersion.current(),
                    queryClusterSettings(),
                    enrichResolver,
                    viewService.getViewResolver(),
                    noDatasetsResolver(),
                    successExecutionInfo,
                    groupIndicesByCluster,
                    runPhase,
                    MOCK_TRANSPORT_ACTION_SERVICES,
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    1,
                    () -> false,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Versioned<Result> result) {}

                        @Override
                        public void onFailure(Exception e) {
                            fail("this shouldn't happen");
                        }
                    }
                );
            }

            // check the new metrics
            assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
            assertEquals(2, planExecutor.metrics().stats().get("queries._all.total"));
            assertEquals(1, planExecutor.metrics().stats().get("features.stats"));
        }
    }

    public void testSettingsMetric() throws Exception {
        IndexResolver indexResolver = mockIndexResolver();

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            var planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            // Initial values should be 0
            assertEquals(0L, planExecutor.metrics().stats().get("settings.time_zone"));
            assertEquals(0L, planExecutor.metrics().stats().get("settings.unmapped_fields"));

            // Run a query with time_zone setting
            var request = new EsqlQueryRequest();
            request.query("SET time_zone=\"UTC\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            final var executionInfo1 = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runTimeZonePhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, executionInfo1)
            );

            executeEsql(planExecutor, request, executionInfo1, runTimeZonePhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen: " + e.getMessage());
                }
            });

            // time_zone should now be 1
            assertEquals(1L, planExecutor.metrics().stats().get("settings.time_zone"));
            assertEquals(0L, planExecutor.metrics().stats().get("settings.unmapped_fields"));

            // Run another query with unmapped_fields setting
            request = new EsqlQueryRequest();
            request.query("SET unmapped_fields=\"NULLIFY\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            final var executionInfo2 = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runUnmappedFieldsPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, executionInfo2)
            );
            executeEsql(planExecutor, request, executionInfo2, runUnmappedFieldsPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen: " + e.getMessage());
                }
            });

            // Both should now have values
            assertEquals(1L, planExecutor.metrics().stats().get("settings.time_zone"));
            assertEquals(1L, planExecutor.metrics().stats().get("settings.unmapped_fields"));

            // Run a query with multiple settings
            request = new EsqlQueryRequest();
            request.query("SET time_zone=\"America/New_York\"; SET unmapped_fields=\"NULLIFY\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            final var executionInfo3 = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runBothSettingsPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, executionInfo3)
            );
            executeEsql(planExecutor, request, executionInfo3, runBothSettingsPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen: " + e.getMessage());
                }
            });

            // Both should be incremented
            assertEquals(2L, planExecutor.metrics().stats().get("settings.time_zone"));
            assertEquals(2L, planExecutor.metrics().stats().get("settings.unmapped_fields"));
        }
    }

    public void testSettingsMetricDeduplication() throws Exception {
        // Verify that when the same setting is SET multiple times in a single query,
        // it's only counted once for telemetry purposes.

        IndexResolver indexResolver = mockIndexResolver();

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            var planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            // Initial value should be 0
            assertEquals(0L, planExecutor.metrics().stats().get("settings.time_zone"));

            // Run a query that SETs time_zone multiple times - should only count once
            var request = new EsqlQueryRequest();
            request.query("SET time_zone=\"UTC\"; SET time_zone=\"America/New_York\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            final var executionInfo1 = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runDedupPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, executionInfo1)
            );

            executeEsql(planExecutor, request, executionInfo1, runDedupPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen: " + e.getMessage());
                }
            });

            // time_zone should be 1, not 2 (deduplicated)
            assertEquals(1L, planExecutor.metrics().stats().get("settings.time_zone"));

            // Run another query with duplicate settings
            request = new EsqlQueryRequest();
            request.query("SET time_zone=\"UTC\"; SET time_zone=\"UTC\"; SET time_zone=\"UTC\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            final var executionInfo2 = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runTripleSetPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(
                createPlanRunnerResult(configuration, executionInfo2)
            );
            executeEsql(planExecutor, request, executionInfo2, runTripleSetPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {}

                @Override
                public void onFailure(Exception e) {
                    fail("this shouldn't happen: " + e.getMessage());
                }
            });

            // time_zone should be 2 (incremented by 1, not 3)
            assertEquals(2L, planExecutor.metrics().stats().get("settings.time_zone"));
        }
    }

    public void testApproximationSettingMetric() throws Exception {
        assumeTrue("approximation setting requires snapshot build", Build.current().isSnapshot());

        IndexResolver indexResolver = mockIndexResolver();

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            var planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            // Initial value should be 0
            assertEquals(0L, planExecutor.metrics().stats().get("settings.approximation"));

            // Run a query with approximation setting
            // Note: When approximation is enabled, the query takes a special execution path via the Approximation class.
            // Skip physical execution: settings metrics are recorded during parsing (see gatherSettingsMetrics) before the runner.
            var request = new EsqlQueryRequest();
            request.query("SET approximation=true; FROM test | STATS COUNT(foo)");
            request.allowPartialResults(false);
            var executionInfo = createEsqlExecutionInfo(randomBoolean());
            EsqlSession.PlanRunner runPhase = (p, configuration, foldContext, planTimeProfile, r) -> r.onFailure(
                new IllegalStateException("skip approximation execution; telemetry collected at parse time")
            );

            executeEsql(planExecutor, request, executionInfo, runPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {
                    // Query might succeed
                }

                @Override
                public void onFailure(Exception e) {
                    // Query might fail during approximation execution phase, but that's OK for this test
                    // The important thing is that the metric was collected during parsing
                }
            });

            // approximation should now be 1 (collected during parsing, regardless of execution outcome)
            assertEquals(1L, planExecutor.metrics().stats().get("settings.approximation"));
        }
    }

    public void testProjectRoutingSettingNotAllowedInStateful() throws Exception {
        // In stateful (non-serverless) mode, project_routing setting should cause a validation error
        // because cross-project search is not enabled.
        // Additionally, the project_routing metric should not be registered at all in stateful mode.

        IndexResolver indexResolver = mockIndexResolver();

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            var planExecutor = buildPlanExecutor(indexResolver, dataSourceModule);

            // In stateful mode, project_routing metric should not be registered at all
            var nestedMap = planExecutor.metrics().stats().toNestedMap();
            @SuppressWarnings("unchecked")
            var settingsMap = (Map<String, Object>) nestedMap.get("settings");
            assertFalse("project_routing metric should not be registered in stateful mode", settingsMap.containsKey("project_routing"));

            // Run a query with project_routing setting - should fail in stateful mode
            var request = new EsqlQueryRequest();
            request.query("SET project_routing=\"test\"; FROM test | KEEP foo");
            request.allowPartialResults(false);
            EsqlSession.PlanRunner runPhase = (p, configuration, foldContext, planTimeProfile, r) -> fail(
                "should not reach execution phase"
            );

            executeEsql(planExecutor, request, createEsqlExecutionInfo(randomBoolean()), runPhase, new ActionListener<>() {
                @Override
                public void onResponse(Versioned<Result> result) {
                    fail("should have failed with validation error");
                }

                @Override
                public void onFailure(Exception e) {
                    // Expected: validation should fail because cross-project search is not enabled
                    assertThat(e, instanceOf(ParsingException.class));
                    assertTrue(e.getMessage().contains("cross-project search not enabled"));
                }
            });
        }
    }

    private IndexResolver mockIndexResolver() {
        return new IndexResolver(mockClient(), () -> true);
    }

    private Client mockClient() {
        String[] indices = new String[] { "test" };
        Client esqlClient = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<EsqlResolveFieldsResponse> listener = (ActionListener<EsqlResolveFieldsResponse>) invocation.getArguments()[2];
            // Must supply non-null resolvedLocally so EsqlResolvedIndexExpression.from() doesn't NPE
            // when CPS is enabled and the flat-index resolution path is taken.
            FieldCapabilitiesResponse fcResponse = FieldCapabilitiesResponse.builder()
                .withIndexResponses(indexFieldCapabilities(indices))
                .withResolvedLocally(new ResolvedIndexExpressions(List.of(), null))
                .build();
            listener.onResponse(new EsqlResolveFieldsResponse(fcResponse));
            return null;
        }).when(esqlClient).execute(eq(EsqlResolveFieldsAction.TYPE), any(), any());
        return esqlClient;
    }

    /**
     * Verifies that {@code in_SET} only increments when {@code project_routing} comes from an in-query
     * {@code SET} clause, not from the request body parameter.
     *
     * <p>Both cases use the same routing expression; the only difference is where it is supplied.
     * {@code planTelemetry.settings()} is populated exclusively from SET-clause settings (via
     * {@code gatherPlanTelemetry}), so request-body routing leaves it empty and {@code setClauseUsed=false}.
     */
    public void testProjectRoutingInSetClause() throws Exception {
        CrossProjectModeDecider cpsDecider = new CrossProjectModeDecider(Settings.EMPTY) {
            @Override
            public boolean crossProjectEnabled() {
                return true;
            }
        };

        Client esqlClient = mockClient();
        IndexResolver indexResolver = new IndexResolver(esqlClient, () -> true);

        try (DataSourceModule dataSourceModule = makeDataSourceModule()) {
            UsageService usageService = new UsageService();
            TransportActionServices services = createTransportActionServices(usageService, cpsDecider);
            PlanExecutor planExecutor = buildPlanExecutor(indexResolver, dataSourceModule, cpsDecider);

            ProjectRoutingRequestInfo routingInfo = ProjectRoutingRequestInfo.NONE;

            // --- Request body: project_routing supplied via request.set(), not the SET clause ---
            var execInfo1 = createEsqlExecutionInfo(true);
            execInfo1.setProjectRoutingInfo(routingInfo, true);

            var request1 = new EsqlQueryRequest();
            request1.query("FROM test | KEEP foo");
            request1.set(QuerySettings.PROJECT_ROUTING, "_alias:_origin");
            request1.allowPartialResults(false);
            final var runPhase1 = planRunnerFor(execInfo1);

            ActionListener<Versioned<Result>> listener = ActionListener.wrap(r -> {}, e -> fail("unexpected failure: " + e.getMessage()));
            executeEsql(planExecutor, services, request1, execInfo1, runPhase1, listener);

            long afterRequestBody = usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlWithSet();
            assertEquals("request-body project_routing must not increment in_SET", 0L, afterRequestBody);
            assertThat(usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlQueriesTotal(), equalTo(1L));

            // --- SET clause: project_routing supplied via the in-query SET command ---
            var execInfo2 = createEsqlExecutionInfo(true);
            execInfo2.setProjectRoutingInfo(routingInfo, true);

            var request2 = new EsqlQueryRequest();
            request2.query("SET project_routing=\"_alias:_origin\"; FROM test | KEEP foo");
            request2.allowPartialResults(false);
            final var runPhase2 = planRunnerFor(execInfo2);

            executeEsql(planExecutor, services, request2, execInfo2, runPhase2, listener);

            long afterSetClause = usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlWithSet();
            assertEquals("SET-clause project_routing must increment in_SET by 1", 1L, afterSetClause);
            assertThat(usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlQueriesTotal(), equalTo(2L));

            // No linked projects - ignore
            var execInfo3 = createEsqlExecutionInfo(true);
            execInfo3.setProjectRoutingInfo(routingInfo, false);

            var request3 = new EsqlQueryRequest();
            request3.query("SET project_routing=\"_alias:_origin\"; FROM test | KEEP foo");
            request3.allowPartialResults(false);
            final var runPhase3 = planRunnerFor(execInfo3);

            executeEsql(planExecutor, services, request3, execInfo3, runPhase3, listener);

            long afterSetClause2 = usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlWithSet();
            assertEquals("Should not increment when no linked projects", 1L, afterSetClause2);
            assertThat(usageService.getProjectRoutingUsageHolder().getSnapshot().getEsqlQueriesTotal(), equalTo(2L));
        }
    }

    private EsqlSession.PlanRunner planRunnerFor(EsqlExecutionInfo executionInfo) {
        return (p, configuration, foldContext, planTimeProfile, r) -> r.onResponse(createPlanRunnerResult(configuration, executionInfo));
    }

    private void executeEsql(
        PlanExecutor planExecutor,
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        EsqlSession.PlanRunner runPhase,
        ActionListener<Versioned<Result>> listener
    ) {
        executeEsql(planExecutor, MOCK_TRANSPORT_ACTION_SERVICES, request, executionInfo, runPhase, listener);
    }

    private void executeEsql(
        PlanExecutor planExecutor,
        TransportActionServices services,
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        EsqlSession.PlanRunner runPhase,
        ActionListener<Versioned<Result>> listener
    ) {
        IndicesExpressionGrouper groupIndicesByCluster = (indicesOptions, indexExpressions, returnLocalAll) -> Map.of(
            "",
            new OriginalIndices(new String[] { "test" }, IndicesOptions.DEFAULT)
        );
        try (InMemoryViewService viewService = InMemoryViewService.makeViewService()) {
            planExecutor.esql(
                request,
                randomAlphaOfLength(10),
                TransportVersion.current(),
                queryClusterSettings(),
                mockEnrichResolver(),
                viewService.getViewResolver(),
                noDatasetsResolver(),
                executionInfo,
                groupIndicesByCluster,
                runPhase,
                services,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                1,
                () -> false,
                listener
            );
        }
    }

    /**
     * These tests register no datasets, so the resolver short-circuits before ever touching a client,
     * executor, or the cross-project remote leg — nulls are never dereferenced.
     */
    private static DatasetResolver noDatasetsResolver() {
        return new DatasetResolver(null, null, CrossProjectModeDecider.NOOP, true);
    }

    private List<FieldCapabilitiesIndexResponse> indexFieldCapabilities(String[] indices) {
        List<FieldCapabilitiesIndexResponse> responses = new ArrayList<>();
        for (String idx : indices) {
            responses.add(
                new FieldCapabilitiesIndexResponse(
                    idx,
                    idx,
                    Map.ofEntries(
                        Map.entry("foo", new IndexFieldCapabilitiesBuilder("foo", "integer").build()),
                        Map.entry("bar", new IndexFieldCapabilitiesBuilder("bar", "long").build())
                    ),
                    true,
                    IndexMode.STANDARD
                )
            );
        }
        return responses;
    }

    private Map<String, Map<String, FieldCapabilities>> fields(String[] indices) {
        FieldCapabilities fooField = new FieldCapabilitiesBuilder("foo", "integer").indices(indices).build();
        FieldCapabilities barField = new FieldCapabilitiesBuilder("bar", "long").indices(indices).build();
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), Map.of(fooField.getName(), fooField));
        fields.put(barField.getName(), Map.of(barField.getName(), barField));
        return fields;
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    private DataSourceModule makeDataSourceModule() {
        List<DataSourcePlugin> plugins = List.of(new DataSourcePlugin() {});
        return new DataSourceModule(
            plugins,
            DataSourceCapabilities.build(plugins),
            Settings.EMPTY,
            blockFactory(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );
    }

    private PlanExecutor buildPlanExecutor(IndexResolver indexResolver, DataSourceModule dataSourceModule) {
        return buildPlanExecutor(indexResolver, dataSourceModule, CrossProjectModeDecider.NOOP);
    }

    private PlanExecutor buildPlanExecutor(
        IndexResolver indexResolver,
        DataSourceModule dataSourceModule,
        CrossProjectModeDecider cpsDecider
    ) {
        return new PlanExecutor(
            indexResolver,
            MeterRegistry.NOOP,
            new XPackLicenseState(() -> 0L),
            mockQueryLog(),
            List.of(),
            cpsDecider,
            dataSourceModule,
            TEST_FUNCTION_REGISTRY,
            PromqlFunctionRegistry.INSTANCE,
            TEST_PARSER,
            null,
            EsqlTestUtils.TEST_ANALYSIS_REGISTRY
        );
    }

    private BlockFactory blockFactory() {
        return BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }
}
