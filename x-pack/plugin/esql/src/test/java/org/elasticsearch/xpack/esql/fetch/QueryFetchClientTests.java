/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocOrigin;
import org.elasticsearch.compute.data.DocOrigins;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * One fetch request as the coordinator sends it: what it carries, and that the drivers of the fetch reach the query
 * exactly once, whatever happens to the request. A missed completion would hang the query, a second one would count the
 * drivers twice.
 */
public class QueryFetchClientTests extends ComputeTestCase {
    private static final DiscoveryNode NODE = DiscoveryNodeUtils.create("n1");
    /**
     * {@link #NODE}, as the fetch operator names the node of a document.
     */
    private static final FetchNode FETCH_NODE = FetchNode.local("n1");
    private static final FetchRequest.ShardDocs SHARD = new FetchRequest.ShardDocs(
        new ShardId("logs-1", "uuid", 0),
        new ShardSearchContextId("session", 7),
        false,
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

    private ThreadPool threadPool;
    private MockTransportService transportService;
    private ClusterService clusterService;
    /**
     * The contexts {@link #tracker} freed, in order.
     */
    private final List<ShardSearchContextId> freed = new CopyOnWriteArrayList<>();
    /**
     * The tracker of the execution of the client. The contexts it tracks are contexts of the node of
     * {@link #transportService}, and it records the free requests instead of sending them.
     */
    private FetchContextTracker tracker;

    @Before
    public void createTracker() {
        threadPool = new TestThreadPool(getTestName());
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        clusterService = ClusterServiceUtils.createClusterService(threadPool, transportService.getLocalNode());
        tracker = new FetchContextTracker(new SearchTransportService(transportService, null, null) {
            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                // the tracker doesn't wait for the answer, so the test doesn't send one
                freed.add(contextId);
            }
        }, clusterService);
    }

    @After
    public void closeTracker() throws IOException {
        tracker.close();
        IOUtils.close(clusterService, transportService);
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * The request is a child of the query's task, carries the index expressions of the query, wildcards included, and loads
     * the attributes of the fetch.
     */
    public void testSendsTheRequestAsAChildOfTheQuery() {
        List<Sent> sent = new ArrayList<>();
        List<Attribute> attributes = List.of(new ReferenceAttribute(Source.EMPTY, "message", DataType.KEYWORD));
        QueryFetchClient client = client((target, request, parentTask, listener) -> sent.add(new Sent(target, request, parentTask)));
        client.fetch(NODE, List.of(SHARD), attributes, ActionListener.noop());

        assertThat(client.threadContext(), sameInstance(threadContext));
        assertThat(sent.size(), equalTo(1));
        assertThat(sent.getFirst().target(), sameInstance(NODE));
        assertThat(sent.getFirst().parentTask(), sameInstance(rootTask));
        FetchRequest request = sent.getFirst().request();
        assertThat(request.sessionId(), equalTo("query-session"));
        assertThat(request.clusterAlias(), equalTo(""));
        assertThat(request.indices(), equalTo(new String[] { "logs-*" }));
        assertThat(request.indicesOptions(), equalTo(SearchRequest.DEFAULT_INDICES_OPTIONS));
        assertThat(request.shards(), equalTo(List.of(SHARD)));
        assertThat(request.attributes(), equalTo(attributes));
    }

    public void testAResponseCountsItsDrivers() {
        FetchResponse response = new FetchResponse(List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<FetchResponse> received = new ArrayList<>();
            client((target, request, parentTask, listener) -> listener.onResponse(response)).fetch(
                NODE,
                List.of(SHARD),
                List.of(),
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
        client((target, request, parentTask, listener) -> listener.onFailure(rejected)).fetch(
            NODE,
            List.of(SHARD),
            List.of(),
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
        QueryFetchClient.Sender cannotSend = (target, request, parentTask, listener) -> { throw new IllegalStateException("not sent"); };
        client(cannotSend).fetch(
            NODE,
            List.of(SHARD),
            List.of(),
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
        FetchResponse response = new FetchResponse(List.of(), FETCH_DRIVERS, 0, 0);
        try {
            List<Exception> failures = new ArrayList<>();
            client((target, request, parentTask, listener) -> {
                try {
                    listener.onResponse(response);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }).fetch(NODE, List.of(SHARD), List.of(), new ActionListener<>() {
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
     * The client finds a node of the local cluster in its cluster state, and fails once the node left.
     */
    public void testFindsTheNodesOfTheLocalCluster() {
        QueryFetchClient client = client((target, request, parentTask, listener) -> fail("nothing to send"));
        assertThat(client.node(FETCH_NODE), sameInstance(NODE));
        expectThrows(NoNodeAvailableException.class, () -> client.node(FetchNode.local("n2")));
    }

    /**
     * The fetch phase reads the local cluster only, so the client refuses a node of a remote cluster, even one with the id of
     * a local node.
     */
    public void testRefusesTheNodesOfARemoteCluster() {
        QueryFetchClient client = client((target, request, parentTask, listener) -> fail("nothing to send"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client.node(new FetchNode("remote", "n1")));
        assertThat(e.getMessage(), equalTo("the fetch phase reads the local cluster only, not node [n1] of [remote]"));
    }

    /**
     * The ends of the input and of the fetch reach the tracker of the execution: the end of the input frees the context no
     * row read, and the end of the fetch the one they read.
     */
    public void testTheEndsOfTheInputAndTheFetchReachTheTracker() {
        QueryFetchClient client = client((target, request, parentTask, listener) -> fail("nothing to send"));
        ShardSearchContextId read = new ShardSearchContextId("session", 1);
        ShardSearchContextId unread = new ShardSearchContextId("session", 2);
        arrived(read, unread);

        client.inputEnded(Set.of(read));
        assertThat(freed, contains(unread));

        client.fetchEnded();
        assertThat(freed, contains(unread, read));
    }

    /**
     * Once the response of the request that reads a context for the last time arrived, its node freed the context, so the
     * tracker no longer frees it.
     */
    public void testTheTrackerLetsGoOfTheContextsTheNodeFreedAfterTheirLastFetch() {
        FetchResponse response = new FetchResponse(List.of(), FETCH_DRIVERS, 0, 0);
        try {
            QueryFetchClient client = client((target, request, parentTask, listener) -> listener.onResponse(response));
            FetchRequest.ShardDocs lastRead = freedAfterFetch(SHARD);
            ShardSearchContextId readAgain = new ShardSearchContextId("session", 8);
            arrived(lastRead.contextId(), readAgain);

            client.fetch(NODE, List.of(lastRead), List.of(), ActionListener.noop());
            client.fetchEnded();

            assertThat(freed, contains(readAgain));
        } finally {
            response.decRef();
        }
    }

    /**
     * A request that failed as a whole may never have reached its node, so the tracker still frees the contexts it read for
     * the last time.
     */
    public void testAFailedLastFetchLeavesItsContextsToTheTracker() {
        QueryFetchClient client = client(
            (target, request, parentTask, listener) -> listener.onFailure(new IllegalStateException("not connected"))
        );
        FetchRequest.ShardDocs lastRead = freedAfterFetch(SHARD);
        arrived(lastRead.contextId());

        client.fetch(NODE, List.of(lastRead), List.of(), ActionListener.noop());
        client.fetchEnded();

        assertThat(freed, contains(lastRead.contextId()));
    }

    private static FetchRequest.ShardDocs freedAfterFetch(FetchRequest.ShardDocs shard) {
        return new FetchRequest.ShardDocs(shard.shardId(), shard.contextId(), true, shard.segments(), shard.docs());
    }

    /**
     * Without index expressions, security would authorize the request for every index the user may read.
     */
    public void testRefusesAFetchWithoutIndexExpressions() {
        OriginalIndices none = new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client((t, r, p, l) -> {}, none));
        assertThat(e.getMessage(), equalTo("a fetch needs the index expressions of its relation"));
    }

    private QueryFetchClient client(QueryFetchClient.Sender sender) {
        // the expressions of a FROM logs-* over the index of SHARD
        return client(sender, new OriginalIndices(new String[] { "logs-*" }, SearchRequest.DEFAULT_INDICES_OPTIONS));
    }

    private QueryFetchClient client(QueryFetchClient.Sender sender, OriginalIndices indices) {
        return new QueryFetchClient(
            sender,
            threadContext,
            () -> DiscoveryNodes.builder().add(NODE).build(),
            new QueryFetchScope(rootTask, "query-session", EsqlTestUtils.TEST_CFG, drivers, tracker),
            indices
        );
    }

    /**
     * A page reached the coordinator whose rows read the contexts {@code ids} of the node of {@link #tracker}.
     */
    private void arrived(ShardSearchContextId... ids) {
        String nodeId = transportService.getLocalNode().getId();
        List<DocOrigin> origins = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            origins.add(new DocOrigin(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, nodeId, new ShardId("logs-1", "uuid", i), ids[i]));
        }
        Page page;
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory(), ids.length)) {
            for (int i = 0; i < ids.length; i++) {
                builder.appendShard(i).appendSegment(0).appendDoc(i);
            }
            page = new Page(builder.build(DocVector.config().origins(DocOrigins.of(origins))));
        }
        try {
            tracker.track(page);
        } finally {
            page.releaseBlocks();
        }
    }

    private record Sent(DiscoveryNode target, FetchRequest request, Task parentTask) {}

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
