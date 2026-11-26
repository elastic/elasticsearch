/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The authorizer's contract: a plan is accepted only when the resolve action, run as the caller, returns every name
 * the plan carries; every other outcome is one and the same refusal. The resolve action is answered by a scripted
 * client, since the decision it encodes (what the security filter narrowed) is the input under test here; the
 * security-enabled behaviour of the real action is covered by the integration tests.
 */
public class ExternalSourceAuthorizerTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final TaskId PARENT = new TaskId("node", 42);

    private ThreadPool threadPool;
    private ScriptedClient client;

    @Before
    public void startThreadPool() {
        threadPool = new TestThreadPool(getTestName());
        client = new ScriptedClient(threadPool);
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testNoExternalNodeIsAcceptedWithoutARoundTrip() {
        authorize(plan(), true);
        assertThat(client.requests, hasSize(0));
    }

    public void testAcceptsWhenEveryNameResolves() {
        client.answer = request -> response(Set.of("a", "b"));
        authorize(plan("a", "b"), randomBoolean());
        assertThat(client.requests, hasSize(1));
        EsqlResolveDatasetAction.Request request = client.requests.get(0);
        assertThat(request.rawPatterns(), arrayContaining("a", "b"));
        assertThat(request.indices(), arrayContaining("a", "b"));
        assertThat(request.wildcardsMatchDatasets(), is(false));
        assertThat(request.getParentTask(), equalTo(PARENT));
    }

    public void testRefusesWhenAnyNameIsMissing() {
        // Unauthorized, outside the grant and unregistered all look the same from here: the name is simply absent.
        client.answer = request -> response(Set.of("a"));
        assertRefused(expectRefusal(plan("a", "b"), randomBoolean()), "b");
    }

    public void testRefusesWhenTheActionFails() {
        // The DLS/FLS interceptor's rejection is a 403 with its own message; neither reaches the caller.
        ElasticsearchSecurityException dlsFls = new ElasticsearchSecurityException(
            "Datasets with document or field level security restrictions are not supported.",
            RestStatus.FORBIDDEN
        );
        client.answer = request -> { throw dlsFls; };
        ElasticsearchStatusException refusal = expectRefusal(plan("a"), randomBoolean());
        assertRefused(refusal, "a");
        assertThat(refusal.getCause(), nullValue());
    }

    public void testFailuresUnrelatedToTheNamesAreNotTurnedIntoARefusal() {
        for (RuntimeException unrelated : List.<RuntimeException>of(
            new TaskCancelledException("by test"),
            new EsRejectedExecutionException("shutting down", true),
            new NodeClosedException(randomDiscoveryNode()),
            new ClusterBlockException(Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL)),
            // a retryable block the resolve action waited on until its timeout
            new ElasticsearchTimeoutException(
                "timed out while waiting for cluster to unblock",
                new ClusterBlockException(Set.of(GatewayService.STATE_NOT_RECOVERED_BLOCK))
            )
        )) {
            client.answer = request -> { throw unrelated; };
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            authorizer(randomBoolean()).authorize(ExternalSourceDatasets.of(plan("a")), PARENT, future);
            Exception e = expectThrows(Exception.class, future::actionGet);
            assertThat(e, sameInstance(unrelated));
        }
    }

    private static DiscoveryNode randomDiscoveryNode() {
        return DiscoveryNodeUtils.create("node", new TransportAddress(TransportAddress.META_ADDRESS, 9300));
    }

    public void testUnnamedNodeIsRefusedOnReleaseAndLetThroughOnSnapshot() {
        PhysicalPlan unnamed = plan((String) null);
        assertRefused(expectRefusal(unnamed, false), "");
        assertThat(client.requests, hasSize(0));

        authorize(unnamed, true);
        assertThat("nothing to authorize", client.requests, hasSize(0));
    }

    public void testUnnamedNodeNextToANamedOneOnSnapshotStillAuthorizesTheName() {
        client.answer = request -> response(Set.of());
        assertRefused(expectRefusal(plan("a", null), true), "a");
        assertThat(client.requests, hasSize(1));
    }

    public void testMalformedNamesAreRefusedWithoutARequest() {
        for (String name : List.of("", "-logs", "logs*", "logs,more", "remote:logs", "logs::failures")) {
            client.requests.clear();
            assertRefused(expectRefusal(plan("ok", name), randomBoolean()), name);
            assertThat("no request for " + name, client.requests, hasSize(0));
        }
    }

    public void testIsExactLocalName() {
        assertThat(ExternalSourceAuthorizer.isExactLocalName("logs"), is(true));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("logs-2024.01.01"), is(true));
        assertThat(ExternalSourceAuthorizer.isExactLocalName(""), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("-logs"), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("logs*"), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("*"), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("a,b"), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("remote:logs"), is(false));
        assertThat(ExternalSourceAuthorizer.isExactLocalName("logs::data"), is(false));
    }

    public void testIsCrossClusterAccess() throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        assertThat("no authentication, security off", authorizer(true).isCrossClusterAccess(), is(false));

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            Authentication.newRealmAuthentication(new User("alice", "reader"), new Authentication.RealmRef("file", "file", "node"))
                .writeToContext(threadContext);
            assertThat(authorizer(true).isCrossClusterAccess(), is(false));
        }

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            crossClusterAccessAuthentication().writeToContext(threadContext);
            assertThat(authorizer(true).isCrossClusterAccess(), is(true));
        }
    }

    /** What a fulfilling cluster sees for a request a querying cluster sent with a cross-cluster API key. */
    private static Authentication crossClusterAccessAuthentication() throws Exception {
        Map<String, Object> apiKeyMetadata = Map.of(
            AuthenticationField.API_KEY_ID_KEY,
            "key-id",
            AuthenticationField.API_KEY_NAME_KEY,
            "key-name",
            AuthenticationField.API_KEY_TYPE_KEY,
            "cross_cluster",
            AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{}"),
            AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{}")
        );
        Authentication apiKey = Authentication.newApiKeyAuthentication(
            AuthenticationResult.success(new User("api-key-user"), apiKeyMetadata),
            "node"
        );
        Authentication querying = Authentication.newRealmAuthentication(
            new User("alice", "reader"),
            new Authentication.RealmRef("file", "file", "querying-node")
        );
        return apiKey.toCrossClusterAccess(new CrossClusterAccessSubjectInfo(querying, RoleDescriptorsIntersection.EMPTY));
    }

    private ExternalSourceAuthorizer authorizer(boolean snapshotBuild) {
        return new ExternalSourceAuthorizer(client, threadPool.getThreadContext(), threadPool.generic(), snapshotBuild);
    }

    private void authorize(PhysicalPlan plan, boolean snapshotBuild) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer(snapshotBuild).authorize(ExternalSourceDatasets.of(plan), PARENT, future);
        future.actionGet();
    }

    private ElasticsearchStatusException expectRefusal(PhysicalPlan plan, boolean snapshotBuild) {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        authorizer(snapshotBuild).authorize(ExternalSourceDatasets.of(plan), PARENT, future);
        return expectThrows(ElasticsearchStatusException.class, future::actionGet);
    }

    private static void assertRefused(ElasticsearchStatusException e, String name) {
        assertThat(e.status(), is(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("external dataset [" + name + "] is not registered on this node"));
    }

    private static EsqlResolveDatasetAction.Response response(Set<String> datasets) {
        return new EsqlResolveDatasetAction.Response(datasets, Set.of(), Set.of());
    }

    private static PhysicalPlan plan(String... datasetNames) {
        List<PhysicalPlan> children = new ArrayList<>();
        for (String name : datasetNames) {
            children.add(
                new ExternalSourceExec(SRC, "s3://bucket/data/*.parquet", "parquet", output(), Map.of(), Map.of(), null, 10)
                    .withDatasetName(name)
            );
        }
        PhysicalPlan child = children.size() == 1 ? children.get(0) : new MergeExec(SRC, children, output(), MergeExec.Kind.UNION);
        return new ExchangeSinkExec(SRC, output(), false, child);
    }

    private static List<Attribute> output() {
        return List.of(new FieldAttribute(SRC, "a", new EsField("a", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)));
    }

    /**
     * Answers {@link EsqlResolveDatasetAction} from {@link #answer} and records the requests. A node client rather than
     * a plain scripted {@code Client} because that is what the production authorizer is handed.
     */
    private static final class ScriptedClient extends NoOpNodeClient {
        final List<EsqlResolveDatasetAction.Request> requests = new ArrayList<>();
        Function<EsqlResolveDatasetAction.Request, EsqlResolveDatasetAction.Response> answer = request -> {
            throw new AssertionError("unexpected resolve request " + Arrays.toString(request.indices()));
        };

        ScriptedClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(EsqlResolveDatasetAction.TYPE));
            EsqlResolveDatasetAction.Request resolve = (EsqlResolveDatasetAction.Request) request;
            requests.add(resolve);
            final EsqlResolveDatasetAction.Response response;
            try {
                response = answer.apply(resolve);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse((Response) response);
        }
    }
}
