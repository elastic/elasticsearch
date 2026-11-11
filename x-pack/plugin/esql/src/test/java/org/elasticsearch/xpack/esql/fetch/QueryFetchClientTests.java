/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * One fetch request as the coordinator sends it: what it carries, and that the drivers of the fetch reach the query
 * exactly once, whatever happens to the request. A missed completion would hang the query, a second one would count the
 * drivers twice.
 */
public class QueryFetchClientTests extends ComputeTestCase {
    private static final DiscoveryNode NODE = DiscoveryNodeUtils.create("n1");
    private static final Map<String, List<String>> INDEX_EXPRESSIONS = Map.of(
        "",
        List.of("logs-*", "-logs-old"),
        "remote",
        List.of("remote-logs-*")
    );
    /**
     * {@link #NODE}, as the lease names the node of its contexts.
     */
    private static final FetchNode FETCH_NODE = FetchNode.local("n1");
    /**
     * The keep-alive {@link #NODE} gives its contexts, longer than the one of the query.
     */
    private static final TimeValue NODE_KEEP_ALIVE = TimeValue.timeValueDays(1);
    private static final FetchRequest.ShardDocs SHARD = new FetchRequest.ShardDocs(
        new ShardId("logs", "uuid", 0),
        new ShardSearchContextId("session", 7),
        FetchRequest.ContextAfterFetch.KEEP,
        new int[] { 0, 0, 2 },
        new int[] { 3, 8, 1 }
    );
    private static final DriverCompletionInfo FETCH_DRIVERS = new DriverCompletionInfo(
        3,
        6,
        3,
        0,
        0,
        0,
        0,
        0,
        List.of(),
        List.of(),
        Map.of(),
        false,
        false,
        Set.of()
    );

    private final CancellableTask rootTask = new CancellableTask(
        1,
        "transport",
        "indices:data/read/esql",
        "",
        TaskId.EMPTY_TASK_ID,
        Map.of()
    );
    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final QueryDrivers drivers = new QueryDrivers();

    /**
     * The request is a child of the query's task, and is authorized with the index expressions of the relation.
     */
    public void testSendsTheRequestAsAChildOfTheQuery() {
        List<Sent> sent = new ArrayList<>();
        PhysicalPlan fetchPlan = FetchRequestTests.fetchPlan(List.of());
        QueryFetchClient client = client(
            (node, target, request, parentTask, listener) -> sent.add(new Sent(node, target, request, parentTask))
        );
        client.fetch(NODE, "", List.of(SHARD), fetchPlan, ActionListener.noop());

        assertThat(client.threadContext(), sameInstance(threadContext));
        assertThat(sent.size(), equalTo(1));
        assertThat(sent.getFirst().node(), equalTo(FETCH_NODE));
        assertThat(sent.getFirst().target(), sameInstance(NODE));
        assertThat(sent.getFirst().parentTask(), sameInstance(rootTask));
        FetchRequest request = sent.getFirst().request();
        assertThat(request.sessionId(), equalTo("query-session"));
        assertThat(request.clusterAlias(), equalTo(""));
        assertThat(request.indices(), equalTo(new String[] { "logs-*", "-logs-old" }));
        assertThat(request.indicesOptions(), equalTo(SearchRequest.DEFAULT_INDICES_OPTIONS));
        assertThat(request.shards(), equalTo(List.of(SHARD)));
        assertThat(request.fetchPlan(), sameInstance(fetchPlan));
    }

    public void testAResponseCountsItsDrivers() {
        FetchResponse response = new FetchResponse(blockFactory(), List.of(), List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<FetchResponse> received = new ArrayList<>();
            client((node, target, request, parentTask, listener) -> listener.onResponse(response)).fetch(
                NODE,
                "",
                List.of(SHARD),
                null,
                ActionListener.assertOnce(ActionListener.wrap(received::add, e -> {
                    throw new AssertionError(e);
                }))
            );
            assertThat(received, contains(sameInstance(response)));
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
        } finally {
            response.decRef();
        }
    }

    public void testAFailedRequestCompletesItsDrivers() {
        List<Exception> failures = new ArrayList<>();
        IllegalStateException rejected = new IllegalStateException("rejected");
        client((node, target, request, parentTask, listener) -> listener.onFailure(rejected)).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
            ActionListener.assertOnce(ActionListener.wrap(r -> fail("unexpected response"), failures::add))
        );
        assertThat(failures, contains(sameInstance(rejected)));
        assertThat(drivers.completed, contains(DriverCompletionInfo.EMPTY));
    }

    /**
     * A request that fails before it leaves the node still completes its listener and its drivers.
     */
    public void testARequestThatCannotBeSentCompletesItsDrivers() {
        List<Exception> failures = new ArrayList<>();
        QueryFetchClient.Sender cannotSend = (node, target, request, parentTask, listener) -> {
            throw new IllegalStateException("not sent");
        };
        client(cannotSend).fetch(
            NODE,
            "",
            List.of(SHARD),
            null,
            ActionListener.assertOnce(ActionListener.wrap(r -> fail("unexpected response"), failures::add))
        );
        assertThat(failures.size(), equalTo(1));
        assertThat(failures.getFirst().getMessage(), equalTo("not sent"));
        assertThat(drivers.completed, contains(DriverCompletionInfo.EMPTY));
    }

    /**
     * When a listener throws, the transport completes it a second time, with the failure. The drivers count once.
     */
    public void testASecondCompletionIsIgnored() {
        FetchResponse response = new FetchResponse(blockFactory(), List.of(), List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<Exception> failures = new ArrayList<>();
            client((node, target, request, parentTask, listener) -> {
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }).fetch(NODE, "", List.of(SHARD), null, new ActionListener<>() {
                @Override
                public void onResponse(FetchResponse fetchResponse) {
                    throw new IllegalStateException("can't read the response");
                }

                @Override
                public void onFailure(Exception e) {
                    failures.add(e);
                }
            });
            assertThat(failures, empty());
            assertThat(drivers.completed, contains(FETCH_DRIVERS));
        } finally {
            response.decRef();
        }
    }

    /**
     * The client finds the nodes through the directory of this node: a node of the local cluster from its cluster state.
     */
    public void testFindsTheNodesOfTheLocalCluster() {
        QueryFetchClient client = client((node, target, request, parentTask, listener) -> fail("nothing to send"));
        PlainActionFuture<DiscoveryNode> found = new PlainActionFuture<>();
        client.node(FETCH_NODE, found);
        assertThat(found.actionGet(), sameInstance(NODE));
        PlainActionFuture<DiscoveryNode> left = new PlainActionFuture<>();
        client.node(FetchNode.local("n2"), left);
        expectThrows(NoNodeAvailableException.class, left::actionGet);
    }

    /**
     * The ends of the cut and of the fetch reach the lease of the query, for the execution of the client: the end of the
     * cut frees the context no row read, and the end of the fetch the one they read.
     */
    public void testTheEndsOfTheCutAndTheFetchReachTheLeaseOfItsExecution() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            RecordedLease recorded = leaseThatRecordsFrees(threadPool);
            QueryFetchClient client = new QueryFetchClient(
                (node, target, request, parentTask, listener) -> fail("nothing to send"),
                threadContext,
                directory(),
                scope(recorded.execution()),
                Map.of("", List.of("logs-*"))
            );
            ShardSearchContextId read = new ShardSearchContextId("session", 1);
            ShardSearchContextId unread = new ShardSearchContextId("session", 2);
            recorded.execution().add(FETCH_NODE, List.of(read, unread), NODE_KEEP_ALIVE);

            client.cutEnded(Set.of(read));
            assertThat(recorded.transport().freed(), equalTo(Set.of(unread)));

            client.fetchEnded();
            assertThat(recorded.transport().freed(), equalTo(Set.of(unread, read)));
            recorded.lease().close();
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Once the response of the request that reads a context for the last time arrived, its node freed the context, so the
     * lease of the execution no longer frees it.
     */
    public void testTheLeaseLetsGoOfTheContextsTheNodeFreedAfterTheirLastFetch() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        List<FetchResponse.ShardResult> loaded = List.of(FetchResponse.ShardResult.succeeded(SHARD.shardId(), SHARD.docCount()));
        FetchResponse response = new FetchResponse(blockFactory(), loaded, List.of(), FETCH_DRIVERS, 0, 0);
        try {
            RecordedLease recorded = leaseThatRecordsFrees(threadPool);
            QueryFetchClient client = new QueryFetchClient(
                (node, target, request, parentTask, listener) -> listener.onResponse(response),
                threadContext,
                directory(),
                scope(recorded.execution()),
                Map.of("", List.of("logs-*"))
            );
            FetchRequest.ShardDocs lastRead = withAfterFetch(SHARD, FetchRequest.ContextAfterFetch.FREE);
            ShardSearchContextId readAgain = new ShardSearchContextId("session", 8);
            recorded.execution().add(FETCH_NODE, List.of(lastRead.contextId(), readAgain), NODE_KEEP_ALIVE);

            client.fetch(NODE, "", List.of(lastRead), null, ActionListener.noop());
            client.fetchEnded();

            assertThat(recorded.transport().freed(), equalTo(Set.of(readAgain)));
            recorded.lease().close();
        } finally {
            response.decRef();
            terminate(threadPool);
        }
    }

    /**
     * A shard can fail before its node opened its context, for example when its index authorization dropped, and then the
     * node frees nothing. The lease still frees the context of a shard that failed.
     */
    public void testAShardThatFailedLeavesItsContextToTheLease() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        List<FetchResponse.ShardResult> failed = List.of(
            FetchResponse.ShardResult.failed(SHARD.shardId(), new IllegalStateException("unauthorized"))
        );
        FetchResponse response = new FetchResponse(blockFactory(), failed, List.of(), FETCH_DRIVERS, 0, 0);
        try {
            RecordedLease recorded = leaseThatRecordsFrees(threadPool);
            QueryFetchClient client = new QueryFetchClient(
                (node, target, request, parentTask, listener) -> listener.onResponse(response),
                threadContext,
                directory(),
                scope(recorded.execution()),
                Map.of("", List.of("logs-*"))
            );
            FetchRequest.ShardDocs lastRead = withAfterFetch(SHARD, FetchRequest.ContextAfterFetch.FREE);
            recorded.execution().add(FETCH_NODE, List.of(lastRead.contextId()), NODE_KEEP_ALIVE);

            client.fetch(NODE, "", List.of(lastRead), null, ActionListener.noop());
            client.fetchEnded();

            assertThat(recorded.transport().freed(), equalTo(Set.of(lastRead.contextId())));
            recorded.lease().close();
        } finally {
            response.decRef();
            terminate(threadPool);
        }
    }

    /**
     * A request that failed as a whole may never have reached its node, so the lease still frees the contexts it read for
     * the last time.
     */
    public void testAFailedLastFetchLeavesItsContextsToTheLease() {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            RecordedLease recorded = leaseThatRecordsFrees(threadPool);
            QueryFetchClient client = new QueryFetchClient(
                (node, target, request, parentTask, listener) -> listener.onFailure(new IllegalStateException("not connected")),
                threadContext,
                directory(),
                scope(recorded.execution()),
                Map.of("", List.of("logs-*"))
            );
            FetchRequest.ShardDocs lastRead = withAfterFetch(SHARD, FetchRequest.ContextAfterFetch.FREE);
            recorded.execution().add(FETCH_NODE, List.of(lastRead.contextId()), NODE_KEEP_ALIVE);

            client.fetch(NODE, "", List.of(lastRead), null, ActionListener.noop());
            client.fetchEnded();

            assertThat(recorded.transport().freed(), equalTo(Set.of(lastRead.contextId())));
            recorded.lease().close();
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A lease and its execution, and the transport that records what the lease frees.
     */
    private record RecordedLease(
        FetchContextLease lease,
        FetchContextLease.Execution execution,
        RecordingFetchContextTransport transport
    ) {}

    private RecordedLease leaseThatRecordsFrees(ThreadPool threadPool) {
        // the service only schedules the refresh of the held contexts on this thread pool, which never comes within the
        // keep-alive of a test. A real one would need a node.
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        RecordingFetchContextTransport transport = new RecordingFetchContextTransport();
        FetchContextLease lease = new FetchContextLease(
            new FetchContextLeases(null, transportService, directory()),
            transport,
            rootTask,
            threadContext.newRestorableContext(false),
            TimeValue.timeValueMinutes(5)
        );
        FetchContextLease.Execution execution = lease.newExecution(
            Map.of("", new OriginalIndices(new String[] { "logs-*" }, SearchRequest.DEFAULT_INDICES_OPTIONS))
        );
        return new RecordedLease(lease, execution, transport);
    }

    private static FetchRequest.ShardDocs withAfterFetch(FetchRequest.ShardDocs shard, FetchRequest.ContextAfterFetch afterFetch) {
        return new FetchRequest.ShardDocs(shard.shardId(), shard.contextId(), afterFetch, shard.segments(), shard.docs());
    }

    /**
     * A request without index expressions would be authorized for every index.
     */
    public void testNeedsTheIndexExpressionsOfTheRelation() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryFetchClient((node, target, request, parentTask, listener) -> {}, threadContext, directory(), scope(), Map.of())
        );
        assertThat(e.getMessage(), containsString("index expressions"));
    }

    /**
     * A cluster with an empty list of index expressions would get requests authorized for every index of that cluster.
     */
    public void testNeedsIndexExpressionsForEveryCluster() {
        Map<String, List<String>> remoteWithout = Map.of("", List.of("logs-*"), "remote", List.of());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryFetchClient(
                (node, target, request, parentTask, listener) -> {},
                threadContext,
                directory(),
                scope(),
                remoteWithout
            )
        );
        assertThat(e.getMessage(), containsString("index expressions"));
    }

    /**
     * A request to a node of a remote cluster names that cluster, and carries the index expressions the query wrote for it,
     * which the remote cluster authorizes it with.
     */
    public void testARequestToARemoteNodeCarriesTheIndexExpressionsOfItsCluster() {
        List<Sent> sent = new ArrayList<>();
        QueryFetchClient client = client(
            (node, target, request, parentTask, listener) -> sent.add(new Sent(node, target, request, parentTask))
        );

        client.fetch(NODE, "remote", List.of(SHARD), null, ActionListener.noop());

        assertThat(sent.size(), equalTo(1));
        assertThat(sent.getFirst().node(), equalTo(new FetchNode("remote", "n1")));
        assertThat(sent.getFirst().request().clusterAlias(), equalTo("remote"));
        assertThat(sent.getFirst().request().indices(), equalTo(new String[] { "remote-logs-*" }));
    }

    /**
     * The relation reads the clusters the documents come from, so a cluster without index expressions is a bug, which
     * fails the fetch rather than sending a request that the cluster authorizes for every index.
     */
    public void testAClusterTheRelationDoesNotReadIsRefused() {
        QueryFetchClient client = client((node, target, request, parentTask, listener) -> fail("nothing to send"));

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> client.fetch(NODE, "other", List.of(SHARD), null, ActionListener.noop())
        );

        assertThat(e.getMessage(), equalTo("the fetch has no index expressions for the cluster [other]"));
        assertThat(drivers.completed, empty());
    }

    private QueryFetchClient client(QueryFetchClient.Sender sender) {
        return new QueryFetchClient(sender, threadContext, directory(), scope(), INDEX_EXPRESSIONS);
    }

    private QueryFetchScope scope() {
        // the client holds no context in these tests, so the lease never sends a request
        return scope(
            new FetchContextLease(
                new FetchContextLeases(null, null, directory()),
                new RecordingFetchContextTransport(),
                rootTask,
                threadContext.newRestorableContext(false),
                TimeValue.timeValueMinutes(5)
            ).newExecution(Map.of("", new OriginalIndices(new String[] { "logs-*" }, SearchRequest.DEFAULT_INDICES_OPTIONS)))
        );
    }

    private QueryFetchScope scope(FetchContextLease.Execution fetchContexts) {
        return new QueryFetchScope(
            rootTask,
            "query-session",
            EsqlTestUtils.TEST_CFG,
            drivers,
            new FetchFailures(false, clusterAlias -> false),
            fetchContexts,
            new FetchPhaseStats()
        );
    }

    private DiscoveryNodes nodes() {
        return DiscoveryNodes.builder().add(NODE).build();
    }

    /**
     * The directory of a node that knows the local cluster only: these tests never look a remote node up.
     */
    private FetchNodeDirectory directory() {
        return new FetchNodeDirectory(
            this::nodes,
            (clusterAlias, listener) -> listener.onFailure(new IllegalStateException("no remote cluster in this test")),
            threadContext
        );
    }

    private record Sent(FetchNode node, DiscoveryNode target, FetchRequest request, Task parentTask) {}

    /**
     * Stands in for the compute listener of the query, which counts the drivers of each listener it hands out.
     */
    private static class QueryDrivers implements Supplier<ActionListener<DriverCompletionInfo>> {
        private final List<DriverCompletionInfo> completed = new ArrayList<>();

        @Override
        public ActionListener<DriverCompletionInfo> get() {
            return ActionListener.assertOnce(ActionListener.wrap(completed::add, e -> { throw new AssertionError(e); }));
        }
    }
}
