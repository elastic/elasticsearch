/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.iplocation.api.IpLocationService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
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
import org.elasticsearch.xpack.esql.into.HasPrivilegesIntoDestinationAuthorizer;
import org.elasticsearch.xpack.esql.into.IntoDestinationAuthorizer;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.querylog.EsqlQueryLog;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_ANALYSIS_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.queryClusterSettings;
import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.querylog.EsqlQueryLogTests.mockLogFieldProvider;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * {@code INTO} reports things about its destination while planning, from cluster state that security does not filter. The session
 * has to authorize the destination first, so that a caller who may not write cannot use those reports to probe it.
 * <p>
 * The tests that run a query go through a real {@link PlanExecutor} and {@link EsqlSession}, with a security that answers
 * {@code has_privileges} from a fixed grant. The destination of those has an ingest pipeline, which INTO refuses for a writer: a
 * caller who sees that refusal was told about the destination, so a denied caller must see the denial instead.
 */
public class EsqlSessionIntoAuthorizationTests extends ESTestCase {
    private static final String EXISTING = "existing";
    private static final String WITHOUT_PIPELINE = "plain";
    private static final String USER = "alice";

    private final List<String> asked = new ArrayList<>();
    private InMemoryViewService viewService;
    private DataSourceModule dataSourceModule;
    private volatile Set<String> granted;
    private volatile Thread plannedOn;
    private ThreadPool threadPool;
    private MockTransportService transportService;

    @Before
    public void setup() throws IOException {
        viewService = InMemoryViewService.makeViewService();
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        AuthenticationTestHelper.builder().user(new User(USER)).build(false).writeToContext(threadPool.getThreadContext());
        transportService.registerRequestHandler(
            HasPrivilegesAction.NAME,
            threadPool.executor(ThreadPool.Names.GENERIC),
            HasPrivilegesRequest::new,
            (request, channel, task) -> {
                var privileges = request.indexPrivileges()[0];
                asked.add(privileges.getIndices()[0]);
                ResourcePrivileges.Builder resource = ResourcePrivileges.builder(privileges.getIndices()[0]);
                for (String privilege : privileges.getPrivileges()) {
                    resource.addPrivilege(privilege, granted.contains(privilege));
                }
                channel.sendResponse(new HasPrivilegesResponse(USER, false, Map.of(), List.of(resource.build()), Map.of()));
            }
        );
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
        transportService.close();
        terminate(threadPool);
    }

    public void testQueryWithoutIntoAsksForNothing() throws Exception {
        authorize(authorizer(false), "ROW x = 1");
        assertThat(asked, empty());
    }

    public void testAuthorizedDestinationPasses() throws Exception {
        authorize(authorizer(true), "ROW x = 1 | INTO " + EXISTING);
        assertThat(asked, contains(EXISTING));
    }

    public void testDeniedDestinationIsForbidden() {
        expectThrows(ElasticsearchSecurityException.class, () -> authorize(authorizer(false), "ROW x = 1 | INTO " + EXISTING));
    }

    public void testDestinationIsAuthorizedByItsResolvedNameAndTheResolvedNameReplacesTheTypedOne() throws Exception {
        LogicalPlan authorized = authorize(authorizer(true), "ROW x = 1 | INTO <logs-{now/d}>");

        assertThat(asked.size(), equalTo(1));
        assertThat(asked.get(0), startsWith("logs-"));
        assertThat(asked.get(0), not(containsString("<")));
        assertThat(asked.get(0), not(containsString("{")));
        // Everything after this, on whichever node, writes to the name that was authorized
        assertThat(authorized.collect(Into.class).get(0).destination(), equalTo(asked.get(0)));
    }

    // Wiring: a real session and what a denied caller can learn from it

    public void testDeniedCallerIsToldNothingAboutTheDestinationThatHasAPipeline() throws Exception {
        assumeInto();
        Exception denied = run("ROW x = 1 | INTO " + EXISTING, Set.of("read"));

        assertThat(denied, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) denied).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(denied.getMessage(), not(containsString("pipeline")));
        assertThat(asked, contains(EXISTING));
    }

    public void testDeniedCallerIsForbiddenForAMissingDestination() throws Exception {
        assumeInto();
        Exception missing = run("ROW x = 1 | INTO nothing", Set.of("read"));

        assertThat(missing, instanceOf(ElasticsearchSecurityException.class));
        assertThat(((ElasticsearchSecurityException) missing).status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(asked, contains("nothing"));
    }

    /** A caller who may write is told what a bulk would tell them: that the destination's pipeline cannot be used. */
    public void testWriterIsToldAboutThePipelineOfTheDestination() throws Exception {
        assumeInto();
        Exception refusal = run("ROW x = 1 | INTO " + EXISTING, Set.of("create_doc"));

        assertThat(refusal, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) refusal).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(refusal.getMessage(), containsString("does not support ingest pipelines"));
    }

    /**
     * The reply to the privileges request arrives on a generic thread, and the planning that continues from it expects the search
     * pool, which it asserts, so reaching the plan is the proof that the decision was moved there.
     */
    public void testWriterOfADestinationWithoutPipelinePlansOnTheSearchPool() throws Exception {
        assumeInto();
        assertNull(run("ROW x = 1 | INTO " + WITHOUT_PIPELINE, Set.of("create_doc")));

        assertThat(asked, contains(WITHOUT_PIPELINE));
        assertThat(plannedOn.getName(), containsString("[" + ThreadPool.Names.SEARCH + "]"));
    }

    public void testQueryWithoutIntoNeverAsksSecurity() throws Exception {
        assertNull(run("ROW x = 1 | LIMIT 1", Set.of()));
        assertThat(asked, empty());
    }

    private IntoDestinationAuthorizer authorizer(boolean grant) {
        return new HasPrivilegesIntoDestinationAuthorizer(() -> "alice", (request, listener) -> {
            var privileges = request.indexPrivileges()[0];
            asked.add(privileges.getIndices()[0]);
            ResourcePrivileges.Builder resource = ResourcePrivileges.builder(privileges.getIndices()[0]);
            for (String privilege : privileges.getPrivileges()) {
                resource.addPrivilege(privilege, grant);
            }
            listener.onResponse(new HasPrivilegesResponse("alice", grant, Map.of(), List.of(resource.build()), Map.of()));
        }, EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    private static ProjectMetadata project() {
        IndexMetadata index = IndexMetadata.builder(EXISTING)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return ProjectMetadata.builder(randomProjectIdOrDefault()).put(index, false).build();
    }

    private static IndexMetadata index(String name, Settings.Builder settings) {
        return IndexMetadata.builder(name)
            .settings(settings.put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /** Authorizes the destinations of the query, returning the plan that results, or throwing what the authorization failed with. */
    private LogicalPlan authorize(IntoDestinationAuthorizer authorizer, String query) throws Exception {
        PlainActionFuture<LogicalPlan> future = new PlainActionFuture<>();
        EsqlSession.authorizeIntoDestinations(authorizer, TEST_PARSER.parseQuery(query), project(), future);
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw (Exception) e.getCause();
        }
    }

    private void assumeInto() {
        assumeTrue("INTO needs a snapshot build", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
    }

    /** What the caller of {@code query} is told: the failure of the query, or {@code null} when it was planned. */
    private Exception run(String query, Set<String> grantedPrivileges) throws Exception {
        granted = grantedPrivileges;
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.allowPartialResults(false);
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
        EsqlSession.PlanRunner runner = (role, plan, configuration, foldContext, planTimeProfile, maxResultSize, listener) -> {
            plannedOn = Thread.currentThread();
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
        try {
            future.get();
            return null;
        } catch (ExecutionException e) {
            return (Exception) e.getCause();
        }
    }

    private PlanExecutor planExecutor() {
        return new PlanExecutor(
            new IndexResolver(mock(Client.class), () -> true),
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
            TEST_ANALYSIS_REGISTRY
        );
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

    /**
     * Services of a node with security on, to the extent the session looks at it: it serves {@code has_privileges} over a real
     * transport service, which answers from {@link #granted} on another thread than the session's, and the request carries an
     * authentication. The cluster has one index, with an ingest pipeline.
     */
    private TransportActionServices transportActionServices() {
        ClusterService clusterService = mock(ClusterService.class);
        doReturn(new ClusterName("test-cluster")).when(clusterService).getClusterName();
        doReturn(Settings.EMPTY).when(clusterService).getSettings();
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(InferenceSettings.getSettings());
        settings.addAll(PlannerSettings.settings());
        settings.addAll(EsqlFlags.ALL_ESQL_FLAGS_SETTINGS);
        doReturn(new ClusterSettings(Settings.EMPTY, settings)).when(clusterService).getClusterSettings();
        // The minimum transport version that batch indexing needs comes from the nodes of the state
        doReturn(ClusterState.builder(ClusterName.DEFAULT).putCompatibilityVersions("node", TransportVersion.current(), Map.of()).build())
            .when(clusterService)
            .state();

        ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(index(EXISTING, Settings.builder().put("index.default_pipeline", "some-pipeline")), false)
            .put(index(WITHOUT_PIPELINE, Settings.builder()), false)
            .build();
        ProjectResolver projectResolver = mock(ProjectResolver.class);
        doReturn(project).when(projectResolver).getProjectMetadata(any(ClusterState.class));
        // Enabled by default in the snapshot builds that INTO needs
        BatchIndexingEnabled batchIndexingEnabled = new BatchIndexingEnabled(
            new ClusterSettings(Settings.EMPTY, Set.of(BatchIndexingEnabled.BATCH_INDEXING))
        );

        return new TransportActionServices(
            mock(Client.class),
            transportService,
            mock(SearchService.class),
            null,
            clusterService,
            projectResolver,
            mock(IndexNameExpressionResolver.class),
            new UsageService(),
            new InferenceService(mock(Client.class), clusterService),
            UserAgentParserRegistry.NOOP,
            IpLocationService.NOOP,
            new BlockFactoryProvider(PlannerUtils.NON_BREAKING_BLOCK_FACTORY),
            new PlannerSettings.Holder(clusterService),
            CrossProjectModeDecider.NOOP,
            batchIndexingEnabled
        );
    }
}
