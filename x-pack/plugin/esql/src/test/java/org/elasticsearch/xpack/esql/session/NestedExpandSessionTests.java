/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.Build;
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
import org.elasticsearch.rest.RestStatus;
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
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.queryClusterSettings;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLogTests.mockLogFieldProvider;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * A query with {@code NESTED_EXPAND} is rejected as soon as it is analyzed, before the plan is optimized and before any part of it
 * runs. Optimization may drop the command (a {@code LIMIT 0} or a {@code WHERE false} above it) and a subquery runs before the main
 * plan is mapped, so a later rejection would let those queries return an empty result, or spend the time of the subquery first.
 * <p>
 * Queries run through a real session. Field caps is the one thing that is not real: its client answers with the mappings of two
 * indices, {@code ne} with a nested field and {@code orders}, and leaves the nested fields out of the answer unless the request asks
 * for them. The runner stands for the execution of a plan and counts how many plans reach it.
 */
public class NestedExpandSessionTests extends ESTestCase {

    private static final String NE = "ne";
    private static final String ORDERS = "orders";

    private InMemoryViewService viewService;
    private ThreadPool fieldCapsThreadPool;
    private DataSourceModule dataSourceModule;
    private final AtomicInteger plansRun = new AtomicInteger();

    @Before
    public void setup() {
        assumeTrue("NESTED_EXPAND is snapshot-only", Build.current().isSnapshot());
        fieldCapsThreadPool = new TestThreadPool(getTestName());
        viewService = InMemoryViewService.makeViewService();
        viewService.addIndex(ProjectId.DEFAULT, NE);
        viewService.addIndex(ProjectId.DEFAULT, ORDERS);
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
        if (dataSourceModule != null) {
            dataSourceModule.close();
        }
        if (viewService != null) {
            viewService.close();
        }
        if (fieldCapsThreadPool != null) {
            terminate(fieldCapsThreadPool);
        }
    }

    public void testSubqueryDoesNotRunBeforeNestedExpandIsRejected() {
        assertRejected("FROM ne | NESTED_EXPAND items | WHERE id IN (FROM orders | KEEP id) | KEEP id");
    }

    public void testSubqueryBeforeNestedExpandDoesNotRunEither() {
        assertRejected("FROM ne | WHERE id IN (FROM orders | KEEP id) | NESTED_EXPAND items | KEEP id");
    }

    public void testNestedExpandBelowLimitZeroIsRejected() {
        assertRejected("FROM ne | NESTED_EXPAND items | LIMIT 0 | KEEP id");
    }

    public void testNestedExpandBelowFalseFilterIsRejected() {
        assertRejected("FROM ne | NESTED_EXPAND items | WHERE false | KEEP id");
    }

    public void testNestedExpandInAnotherBranchIsRejected() {
        assertRejected("FROM ne | FORK (KEEP id) (NESTED_EXPAND items | KEEP id) | KEEP id");
    }

    public void testSameSubqueryRunsWithoutNestedExpand() {
        run("FROM ne | WHERE id IN (FROM orders | KEEP id) | KEEP id");
        assertThat(plansRun.get(), greaterThan(1));
    }

    private void assertRejected(String query) {
        VerificationException e = expectThrows(VerificationException.class, () -> run(query));
        assertThat(query, e.getMessage(), containsString("NESTED_EXPAND is not supported yet"));
        assertThat(query, e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(query, plansRun.get(), equalTo(0));
    }

    private void run(String query) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query + " | LIMIT 10");
        request.allowPartialResults(false);
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
        EsqlSession.PlanRunner runner = (role, plan, configuration, foldContext, planTimeProfile, maxResultSize, listener) -> {
            plansRun.incrementAndGet();
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

    private Client fieldCapsClient() {
        Client client = mock(Client.class);
        doAnswer((Answer<Void>) invocation -> {
            EsqlResolveFieldsRequest request = invocation.getArgument(1);
            ActionListener<EsqlResolveFieldsResponse> listener = invocation.getArgument(2);
            boolean nestedFieldsAsked = Arrays.asList(request.fieldCapsRequest().filters()).contains("-nested") == false;
            List<FieldCapabilitiesIndexResponse> indices = new ArrayList<>();
            for (String index : request.fieldCapsRequest().indices()) {
                indices.add(
                    new FieldCapabilitiesIndexResponse(
                        index,
                        index,
                        mapping(index, nestedFieldsAsked),
                        true,
                        IndexMode.STANDARD,
                        0,
                        0,
                        0,
                        0
                    )
                );
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

    private static Map<String, IndexFieldCapabilities> mapping(String index, boolean nestedFieldsAsked) {
        Map<String, IndexFieldCapabilities> fields = new HashMap<>();
        fields.put("id", new IndexFieldCapabilitiesBuilder("id", "keyword").build());
        if (index.equals(NE) && nestedFieldsAsked) {
            fields.put("items", new IndexFieldCapabilitiesBuilder("items", "nested").build());
            fields.put("items.sku", new IndexFieldCapabilitiesBuilder("items.sku", "keyword").build());
        }
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
            mock(Client.class),
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
