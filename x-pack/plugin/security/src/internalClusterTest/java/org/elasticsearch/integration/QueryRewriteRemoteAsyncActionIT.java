/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryRewriteRemoteAsyncAction;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_USER_NAME;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.MONITORING_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

@ESTestCase.WithoutEntitlements
public class QueryRewriteRemoteAsyncActionIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_A = "cluster-a";
    private static final String REMOTE_CLUSTER_B = "cluster-b";

    private static final String INDEX_1 = "index-1";
    private static final String INDEX_2 = "index-2";

    private static final ConcurrentHashMap<String, AtomicInteger> INSTRUMENTED_ACTION_CALL_MAP = new ConcurrentHashMap<>();

    private final boolean securityEnabled;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B);
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_A, true, REMOTE_CLUSTER_B, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(TestPlugin.class);
    }

    @Override
    protected NodeConfigurationSource nodeConfigurationSource() {
        return securityEnabled ? new CustomSecuritySettingsSource(false, createTempDir(), ESIntegTestCase.Scope.TEST) : null;
    }

    @Override
    protected String internalClientOrigin() {
        return MONITORING_ORIGIN;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        INSTRUMENTED_ACTION_CALL_MAP.clear();
        setupClusters();
    }

    @After
    public void cleanupSecurityIndex() {
        if (securityEnabled) {
            deleteSecurityIndex(LOCAL_CLUSTER);
            for (String clusterAlias : remoteClusterAlias()) {
                deleteSecurityIndex(clusterAlias);
            }
        }
    }

    public QueryRewriteRemoteAsyncActionIT(boolean securityEnabled) {
        this.securityEnabled = securityEnabled;
    }

    public void testCallRemoteAsyncActionWithOrigin() {
        SearchRequestBuilder allClustersAllIndicesRequest = buildSearchRequest(
            List.of(INDEX_1, INDEX_2),
            List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B),
            ML_ORIGIN
        );
        assertSearchResponse(allClustersAllIndicesRequest);
        assertInstrumentedActionCalls(2, 2);

        SearchRequestBuilder allClustersSingleIndexRequest = buildSearchRequest(
            List.of(INDEX_1),
            List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B),
            ML_ORIGIN
        );
        assertSearchResponse(allClustersSingleIndexRequest);
        assertInstrumentedActionCalls(3, 3);

        SearchRequestBuilder singleClusterSingleIndexRequest = buildSearchRequest(List.of(INDEX_1), List.of(REMOTE_CLUSTER_A), ML_ORIGIN);
        assertSearchResponse(singleClusterSingleIndexRequest);
        assertInstrumentedActionCalls(4, 3);
    }

    public void testCallRemoteAsyncActionWithoutOrigin() {
        Consumer<SearchRequestBuilder> assertSecurityEnabled = r -> {
            assertSearchFailure(
                r,
                ElasticsearchSecurityException.class,
                "action [cluster:internal/test/instrumented] is unauthorized for user [test_user] with effective roles [user]"
            );
            assertInstrumentedActionCalls(0, 0);
        };

        SearchRequestBuilder allClustersAllIndicesRequest = buildSearchRequest(
            List.of(INDEX_1, INDEX_2),
            List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B),
            null
        );
        if (securityEnabled) {
            assertSecurityEnabled.accept(allClustersAllIndicesRequest);
        } else {
            assertSearchResponse(allClustersAllIndicesRequest);
            assertInstrumentedActionCalls(2, 2);
        }

        SearchRequestBuilder allClustersSingleIndexRequest = buildSearchRequest(
            List.of(INDEX_1),
            List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B),
            null
        );
        if (securityEnabled) {
            assertSecurityEnabled.accept(allClustersSingleIndexRequest);
        } else {
            assertSearchResponse(allClustersSingleIndexRequest);
            assertInstrumentedActionCalls(3, 3);
        }

        SearchRequestBuilder singleClusterSingleIndexRequest = buildSearchRequest(List.of(INDEX_1), List.of(REMOTE_CLUSTER_A), null);
        if (securityEnabled) {
            assertSecurityEnabled.accept(singleClusterSingleIndexRequest);
        } else {
            assertSearchResponse(singleClusterSingleIndexRequest);
            assertInstrumentedActionCalls(4, 3);
        }
    }

    public void testInvalidClusterAlias() {
        SearchRequestBuilder request = buildSearchRequest(
            List.of(INDEX_1, INDEX_2),
            List.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B, "missing-cluster-alias"),
            null
        );
        assertSearchFailure(request, NoSuchRemoteClusterException.class, "no such remote cluster: [missing-cluster-alias]");
        assertInstrumentedActionCalls(0, 0);
    }

    private void setupClusters() {
        setupCluster(LOCAL_CLUSTER);
        setupCluster(REMOTE_CLUSTER_A);
        setupCluster(REMOTE_CLUSTER_B);
    }

    private void setupCluster(String clusterAlias) {
        final Client client = client(clusterAlias);
        assertAcked(client.admin().indices().prepareCreate(INDEX_1));
        assertAcked(client.admin().indices().prepareCreate(INDEX_2));
    }

    private void deleteSecurityIndex(String clusterAlias) {
        final Client client = new OriginSettingClient(client(clusterAlias), SECURITY_ORIGIN);

        GetIndexRequest getIndexRequest = new GetIndexRequest(TEST_REQUEST_TIMEOUT);
        getIndexRequest.indices(SECURITY_MAIN_ALIAS);
        getIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        GetIndexResponse getIndexResponse = client.admin().indices().getIndex(getIndexRequest).actionGet(TEST_REQUEST_TIMEOUT);

        if (getIndexResponse.getIndices().length > 0) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(getIndexResponse.getIndices());
            assertAcked(client.admin().indices().delete(deleteIndexRequest).actionGet(TEST_REQUEST_TIMEOUT));
        }
    }

    private SearchRequestBuilder buildSearchRequest(List<String> indices, List<String> clusterAliases, @Nullable String origin) {
        Client client = client();
        if (securityEnabled) {
            client = client.filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(TEST_USER_NAME, TEST_PASSWORD_SECURE_STRING)));
        }

        return client.prepareSearch(generateFullyQualifiedIndices(indices, clusterAliases)).setQuery(new TestQueryBuilder(origin));
    }

    private static String[] generateFullyQualifiedIndices(List<String> indices, List<String> clusterAliases) {
        String[] fullyQualifiedIndices = new String[indices.size() * clusterAliases.size()];

        int idx = 0;
        for (String clusterAlias : clusterAliases) {
            for (String index : indices) {
                StringBuilder fullyQualifiedIndex = new StringBuilder();
                if (LOCAL_CLUSTER.equals(clusterAlias) == false) {
                    fullyQualifiedIndex.append(clusterAlias);
                    fullyQualifiedIndex.append(":");
                }
                fullyQualifiedIndex.append(index);

                fullyQualifiedIndices[idx++] = fullyQualifiedIndex.toString();
            }
        }

        return fullyQualifiedIndices;
    }

    private static void assertSearchResponse(SearchRequestBuilder searchRequest) {
        assertResponse(searchRequest, response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo(0L));
            assertThat(response.getSuccessfulShards(), equalTo(response.getTotalShards()));
            assertThat(
                response.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
                equalTo(response.getClusters().getTotal())
            );
        });
    }

    private static <T extends Exception> void assertSearchFailure(
        SearchRequestBuilder searchRequest,
        Class<T> expectedExceptionClass,
        String expectedMessage
    ) {
        T actualException = assertThrows(expectedExceptionClass, () -> assertResponse(searchRequest, response -> {}));
        assertThat(actualException.getMessage(), containsString(expectedMessage));
    }

    private static void assertInstrumentedActionCalls(int expectedClusterACalls, int expectedClusterBCalls) {
        Map<String, Integer> expected = new HashMap<>();
        if (expectedClusterACalls > 0) {
            expected.put(REMOTE_CLUSTER_A, expectedClusterACalls);
        }
        if (expectedClusterBCalls > 0) {
            expected.put(REMOTE_CLUSTER_B, expectedClusterBCalls);
        }

        Map<String, Integer> actual = INSTRUMENTED_ACTION_CALL_MAP.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));

        assertThat(actual, equalTo(expected));
    }

    private static final class TestQueryRewriteRemoteAsyncAction extends QueryRewriteRemoteAsyncAction<
        Void,
        TestQueryRewriteRemoteAsyncAction> {

        private final int incrementCount;
        private final String origin;

        private TestQueryRewriteRemoteAsyncAction(String clusterAlias, int incrementCount, @Nullable String origin) {
            super(clusterAlias);
            this.incrementCount = incrementCount;
            this.origin = origin;
        }

        @Override
        protected void execute(RemoteClusterClient client, ThreadContext threadContext, ActionListener<Void> listener) {
            ActionListener<InstrumentedAction.Response> wrappedListener = listener.delegateFailureAndWrap((l, resp) -> {
                if (resp.isAcknowledged()) {
                    l.onResponse(null);
                } else {
                    l.onFailure(new IllegalStateException("Unacknowledged response from cluster [" + getClusterAlias() + "]"));
                }
            });

            InstrumentedAction.Request request = new InstrumentedAction.Request(incrementCount);
            BiConsumer<InstrumentedAction.Request, ActionListener<InstrumentedAction.Response>> requestConsumer = (req, l) -> {
                client.execute(InstrumentedAction.REMOTE_TYPE, req, l);
            };

            if (origin != null) {
                executeAsyncWithOrigin(threadContext, origin, request, wrappedListener, requestConsumer);
            } else {
                requestConsumer.accept(request, wrappedListener);
            }
        }

        @Override
        public int doHashCode() {
            return Objects.hash(incrementCount, origin);
        }

        @Override
        public boolean doEquals(TestQueryRewriteRemoteAsyncAction other) {
            return Objects.equals(incrementCount, other.incrementCount) && Objects.equals(origin, other.origin);
        }
    }

    private static final class TestQueryBuilder extends AbstractQueryBuilder<TestQueryBuilder> {
        private static final String NAME = "test";

        private final String origin;
        private final Boolean actionsAcknowledged;
        private final ActionFuture<Boolean> actionsAcknowledgedSupplier;

        private static TestQueryBuilder fromXContent(XContentParser parser) {
            return new TestQueryBuilder();
        }

        private TestQueryBuilder() {
            this((String) null);
        }

        private TestQueryBuilder(@Nullable String origin) {
            this.origin = origin;
            this.actionsAcknowledged = null;
            this.actionsAcknowledgedSupplier = null;
        }

        private TestQueryBuilder(StreamInput in) throws IOException {
            super(in);
            this.origin = in.readOptionalString();
            this.actionsAcknowledged = in.readOptionalBoolean();
            this.actionsAcknowledgedSupplier = null;
        }

        private TestQueryBuilder(TestQueryBuilder other, Boolean actionsAcknowledged, ActionFuture<Boolean> actionsAcknowledgedSupplier) {
            this.origin = other.origin;
            this.actionsAcknowledged = actionsAcknowledged;
            this.actionsAcknowledgedSupplier = actionsAcknowledgedSupplier;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            if (actionsAcknowledgedSupplier != null) {
                throw new IllegalStateException(
                    "actionsAcknowledgedSupplier must be null, can't serialize suppliers, missing a rewriteAndFetch?"
                );
            }

            out.writeOptionalString(origin);
            out.writeOptionalBoolean(this.actionsAcknowledged);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME);
            builder.endObject();
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            ResolvedIndices resolvedIndices = queryRewriteContext.getResolvedIndices();
            if (resolvedIndices != null) {
                TestQueryBuilder rewritten = this;

                if (actionsAcknowledgedSupplier != null) {
                    Boolean actionsAcknowledged = actionsAcknowledgedSupplier.isDone() ? actionsAcknowledgedSupplier.actionGet() : null;
                    if (actionsAcknowledged != null) {
                        rewritten = new TestQueryBuilder(this, actionsAcknowledged, null);
                    }
                } else if (actionsAcknowledged == null) {
                    ActionFuture<Boolean> actionsAcknowledgedSupplier = registerActions(queryRewriteContext, origin);
                    rewritten = new TestQueryBuilder(this, null, actionsAcknowledgedSupplier);
                }

                return rewritten;
            }

            return this;
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            assertThat(actionsAcknowledged, is(true));
            return Queries.NO_DOCS_INSTANCE;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        protected boolean doEquals(TestQueryBuilder other) {
            return Objects.equals(this.origin, other.origin)
                && Objects.equals(this.actionsAcknowledged, other.actionsAcknowledged)
                && Objects.equals(this.actionsAcknowledgedSupplier, other.actionsAcknowledgedSupplier);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(origin, actionsAcknowledged, actionsAcknowledgedSupplier);
        }

        private static ActionFuture<Boolean> registerActions(QueryRewriteContext queryRewriteContext, String origin) {
            var remoteClusterIndices = queryRewriteContext.getResolvedIndices().getRemoteClusterIndices();

            final int actionCount = remoteClusterIndices.size();
            Map<String, Integer> clusterIndicesCountMap = new HashMap<>();
            for (var entry : remoteClusterIndices.entrySet()) {
                String clusterAlias = entry.getKey();
                OriginalIndices originalIndices = entry.getValue();

                int indicesCount = originalIndices.indices().length;
                clusterIndicesCountMap.put(clusterAlias, indicesCount);
            }

            PlainActionFuture<Boolean> actionsAcknowledgedSupplier = new PlainActionFuture<>();
            GroupedActionListener<Void> gal = new GroupedActionListener<>(
                actionCount,
                ActionListener.wrap(c -> actionsAcknowledgedSupplier.onResponse(true), actionsAcknowledgedSupplier::onFailure)
            );

            for (var entry : clusterIndicesCountMap.entrySet()) {
                String clusterAlias = entry.getKey();
                Integer indicesCount = entry.getValue();

                queryRewriteContext.registerUniqueAsyncAction(
                    new TestQueryRewriteRemoteAsyncAction(clusterAlias, indicesCount, origin),
                    v -> gal.onResponse(null)
                );
            }

            return actionsAcknowledgedSupplier;
        }
    }

    public static class InstrumentedAction extends ActionType<InstrumentedAction.Response> {
        private static final InstrumentedAction INSTANCE = new InstrumentedAction();
        private static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(INSTANCE.name(), Response::new);

        private static final String NAME = "cluster:internal/test/instrumented";

        private InstrumentedAction() {
            super(NAME);
        }

        public static class Request extends ActionRequest {
            private final int incrementCount;

            public Request(int incrementCount) {
                this.incrementCount = incrementCount;
            }

            public Request(StreamInput in) throws IOException {
                super(in);
                this.incrementCount = in.readInt();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeInt(incrementCount);
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Request other = (Request) o;
                return incrementCount == other.incrementCount;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(incrementCount);
            }
        }

        public static class Response extends AcknowledgedResponse {
            public Response() {
                super(true);
            }

            public Response(StreamInput in) throws IOException {
                super(in);
            }
        }
    }

    public static class TransportInstrumentedAction extends HandledTransportAction<
        InstrumentedAction.Request,
        InstrumentedAction.Response> {
        private final ClusterService clusterService;

        @Inject
        public TransportInstrumentedAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
            super(
                InstrumentedAction.NAME,
                transportService,
                actionFilters,
                InstrumentedAction.Request::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, InstrumentedAction.Request request, ActionListener<InstrumentedAction.Response> listener) {
            String clusterName = clusterService.getClusterName().value();
            AtomicInteger callCounter = INSTRUMENTED_ACTION_CALL_MAP.computeIfAbsent(clusterName, k -> new AtomicInteger());
            callCounter.getAndAdd(request.incrementCount);

            listener.onResponse(new InstrumentedAction.Response());
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin, SearchPlugin {
        public TestPlugin() {}

        @Override
        public Collection<ActionHandler> getActions() {
            return List.of(new ActionHandler(InstrumentedAction.INSTANCE, TransportInstrumentedAction.class));
        }

        @Override
        public List<QuerySpec<?>> getQueries() {
            return List.of(new QuerySpec<QueryBuilder>(TestQueryBuilder.NAME, TestQueryBuilder::new, TestQueryBuilder::fromXContent));
        }
    }

    private static class CustomSecuritySettingsSource extends SecuritySettingsSource {
        private static final String TEST_ROLE_YML = """
            user:
              cluster: [ NONE ]
              indices:
                - names: 'index-*'
                  allow_restricted_indices: false
                  privileges: [ ALL ]
            """;

        private static final String CONFIG_STANDARD_ROLES_YML = TEST_ROLE_YML + "\n" + SecuritySettingsSourceField.ES_TEST_ROOT_ROLE_YML;

        private CustomSecuritySettingsSource(boolean sslEnabled, Path parentFolder, ESIntegTestCase.Scope scope) {
            super(sslEnabled, parentFolder, scope);
        }

        @Override
        protected String configRoles() {
            return CONFIG_STANDARD_ROLES_YML;
        }
    }
}
