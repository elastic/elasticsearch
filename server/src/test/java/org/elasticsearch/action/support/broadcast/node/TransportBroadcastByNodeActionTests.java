/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.broadcast.node;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ReachabilityChecker;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.object.HasToString.hasToString;

public class TransportBroadcastByNodeActionTests extends ESTestCase {

    private static final String TEST_INDEX = "test-index";
    private static final String TEST_CLUSTER = "test-cluster";
    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;

    private TestTransportBroadcastByNodeAction action;
    private TransportService transportService;

    public static class Request extends BroadcastRequest<Request> {
        private final RefCounted refCounted = AbstractRefCounted.of(() -> {});

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(String... indices) {
            super(indices);
        }

        @Override
        public void incRef() {
            refCounted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return refCounted.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return refCounted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return refCounted.hasReferences();
        }

        @Override
        public String toString() {
            return "testrequest" + Arrays.toString(indices);
        }
    }

    public static class Response extends BaseBroadcastResponse {
        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures) {
            super(totalShards, successfulShards, failedShards, shardFailures);
        }
    }

    // empty per-shard result, but not a singleton so we can check each instance is released on cancellation
    public static class ShardResult implements Writeable {
        public ShardResult() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    class TestTransportBroadcastByNodeAction extends TransportBroadcastByNodeAction<Request, Response, ShardResult> {
        private final Map<ShardRouting, Object> shards = new HashMap<>();

        TestTransportBroadcastByNodeAction(String actionName) {
            super(
                actionName,
                TransportBroadcastByNodeActionTests.this.clusterService,
                TransportBroadcastByNodeActionTests.this.transportService,
                new ActionFilters(Set.of()),
                new MyResolver(),
                Request::new,
                TransportBroadcastByNodeActionTests.this.transportService.getThreadPool().executor(TEST_THREAD_POOL_NAME)
            );
        }

        @Override
        protected ShardResult readShardResult(StreamInput in) {
            return new ShardResult();
        }

        @Override
        protected ResponseFactory<Response, ShardResult> getResponseFactory(Request request, ClusterState clusterState) {
            return (totalShards, successfulShards, failedShards, emptyResults, shardFailures) -> new Response(
                totalShards,
                successfulShards,
                failedShards,
                shardFailures
            );
        }

        @Override
        protected Request readRequestFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<ShardResult> listener) {
            ActionListener.completeWith(listener, () -> {
                if (rarely()) {
                    shards.put(shardRouting, Boolean.TRUE);
                    return new ShardResult();
                } else {
                    ElasticsearchException e = new ElasticsearchException("operation failed");
                    shards.put(shardRouting, e);
                    throw e;
                }
            });
        }

        @Override
        protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
            return clusterState.routingTable().allShards(new String[] { TEST_INDEX });
        }

        @Override
        protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
        }

        public Map<ShardRouting, Object> getResults() {
            return shards;
        }

    }

    static class MyResolver extends IndexNameExpressionResolver {
        MyResolver() {
            super(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        }

        @Override
        public String[] concreteIndexNames(ProjectMetadata project, IndicesRequest request) {
            return request.indices();
        }
    }

    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";

    private static void awaitForkedTasks() {
        safeAwait(listener -> THREAD_POOL.executor(TEST_THREAD_POOL_NAME).execute(ActionRunnable.run(listener, () -> {})));
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(
            TransportBroadcastByNodeActionTests.class.getSimpleName(),
            new ScalingExecutorBuilder(TEST_THREAD_POOL_NAME, 1, 1, TimeValue.timeValueSeconds(60), true)
        );
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        setClusterState(clusterService);
        action = new TestTransportBroadcastByNodeAction("indices:admin/test");
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    void setClusterState(ClusterService clusterService) {
        int numberOfNodes = randomIntBetween(3, 5);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(new Index(TEST_INDEX, "_na_"));

        int shardIndex = -1;
        int totalIndexShards = 0;
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            int numberOfShards = randomIntBetween(1, 10);
            totalIndexShards += numberOfShards;
            for (int j = 0; j < numberOfShards; j++) {
                final ShardId shardId = new ShardId(TEST_INDEX, "_na_", ++shardIndex);
                ShardRouting shard = TestShardRouting.newShardRouting(
                    TEST_INDEX,
                    shardId.getId(),
                    node.getId(),
                    true,
                    ShardRoutingState.STARTED
                );
                IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(shardId);
                indexShard.addShard(shard);
                indexRoutingTable.addIndexShard(indexShard);
            }
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.masterNodeId(newNode(numberOfNodes - 1).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName(TEST_CLUSTER));
        stateBuilder.nodes(discoBuilder);
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder(TEST_INDEX)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfReplicas(0)
            .numberOfShards(totalIndexShards);

        stateBuilder.metadata(Metadata.builder().put(indexMetadata));
        stateBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable.build()).build());
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    static DiscoveryNode newNode(int nodeId) {
        return DiscoveryNodeUtils.builder("node_" + nodeId).roles(emptySet()).build();
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        Request request = new Request(TEST_INDEX);

        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));

        assertEquals(
            "blocked by: [SERVICE_UNAVAILABLE/1/test-block];",
            safeAwaitFailure(ClusterBlockException.class, Response.class, listener -> action.doExecute(null, request, listener))
                .getMessage()
        );
    }

    public void testRequestBlock() {
        Request request = new Request(TEST_INDEX);

        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addIndexBlock(
                TEST_INDEX,
                new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL)
            );
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        assertEquals(
            "index [" + TEST_INDEX + "] blocked by: [SERVICE_UNAVAILABLE/1/test-block];",
            safeAwaitFailure(ClusterBlockException.class, Response.class, listener -> action.doExecute(null, request, listener))
                .getMessage()
        );
    }

    public void testOneRequestIsSentToEachNodeHoldingAShard() {
        Request request = new Request(TEST_INDEX);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        action.doExecute(null, request, listener);
        request.decRef(); // before the forked tasks complete
        awaitForkedTasks();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            set.add(shard.currentNodeId());
        }

        // check a request was sent to the right number of nodes
        assertEquals(set.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each node
            assertEquals(1, entry.getValue().size());
            assertThat(
                entry.getValue().iterator().next().request().toString(),
                allOf(
                    containsString('[' + action.transportNodeBroadcastAction + ']'),
                    containsString('[' + entry.getKey() + ']'),
                    containsString("[testrequest[" + TEST_INDEX + "]]")
                )
            );
        }

        assertFalse(request.hasReferences());
        assertTrue(capturedRequests.values().stream().flatMap(Collection::stream).noneMatch(cr -> cr.request().hasReferences()));
    }

    public void testNoShardOperationsExecutedIfTaskCancelled() throws Exception {
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<ShardRouting> shards = new HashSet<>();
        String nodeId = shardIt.iterator().next().currentNodeId();
        for (ShardRouting shard : shardIt) {
            if (nodeId.equals(shard.currentNodeId())) {
                shards.add(shard);
            }
        }
        final TransportBroadcastByNodeAction<Request, Response, ShardResult>.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();

        final PlainActionFuture<TransportResponse> future = new PlainActionFuture<>();
        TestTransportChannel channel = new TestTransportChannel(future);

        final CancellableTask cancellableTask = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());
        TaskCancelHelper.cancel(cancellableTask, "simulated");
        handler.messageReceived(action.new NodeRequest(new Request(), new ArrayList<>(shards), nodeId), channel, cancellableTask);
        expectThrows(TaskCancelledException.class, future::actionGet);

        assertThat(action.getResults(), anEmptyMap());
    }

    // simulate the master being removed from the cluster but before a new master is elected
    // as such, the shards assigned to the master will still show up in the cluster state as assigned to a node but
    // that node will not be in the local cluster state on any node that has detected the master as failing
    // in this case, such a shard should be treated as unassigned
    public void testRequestsAreNotSentToFailedMaster() {
        Request request = new Request(TEST_INDEX);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        DiscoveryNode masterNode = clusterService.state().nodes().getMasterNode();
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
        builder.remove(masterNode.getId()).masterNodeId(null);

        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));

        action.doExecute(null, request, listener);
        awaitForkedTasks();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        // the master should not be in the list of nodes that requests were sent to
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            if (shard.currentNodeId().equals(masterNode.getId()) == false) {
                set.add(shard.currentNodeId());
            }
        }

        // check a request was sent to the right number of nodes
        assertEquals(set.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each non-master node
            assertEquals(1, entry.getValue().size());
        }
    }

    public void testOperationExecution() throws Exception {
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<ShardRouting> shards = new HashSet<>();
        String nodeId = shardIt.iterator().next().currentNodeId();
        for (ShardRouting shard : shardIt) {
            if (nodeId.equals(shard.currentNodeId())) {
                shards.add(shard);
            }
        }
        final TransportBroadcastByNodeAction<Request, Response, ShardResult>.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();

        final PlainActionFuture<TransportResponse> future = new PlainActionFuture<>();
        TestTransportChannel channel = new TestTransportChannel(future);

        handler.messageReceived(action.new NodeRequest(new Request(), new ArrayList<>(shards), nodeId), channel, null);

        // check the operation was executed only on the expected shards
        assertEquals(shards, action.getResults().keySet());

        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        @SuppressWarnings("rawtypes")
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        // check the operation was executed on the correct node
        assertEquals("node id", nodeId, nodeResponse.getNodeId());

        int successfulShards = 0;
        int failedShards = 0;
        for (Object result : action.getResults().values()) {
            if ((result instanceof ElasticsearchException) == false) {
                successfulShards++;
            } else {
                failedShards++;
            }
        }
        // check the operation results
        assertEquals("successful shards", successfulShards, nodeResponse.getSuccessfulShards());
        assertEquals("total shards", action.getResults().size(), nodeResponse.getTotalShards());
        @SuppressWarnings("unchecked")
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        assertEquals("exceptions count", failedShards, exceptions.size());
        for (BroadcastShardOperationFailedException exception : exceptions) {
            assertThat(exception.getMessage(), is("operation indices:admin/test failed"));
            assertThat(exception.getCause(), hasToString(containsString("operation failed")));
        }
    }

    public void testResultAggregation() throws ExecutionException, InterruptedException {
        Request request = new Request(TEST_INDEX);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        // simulate removing the master
        final boolean simulateFailedMasterNode = rarely();
        DiscoveryNode failedMasterNode = null;
        if (simulateFailedMasterNode) {
            failedMasterNode = clusterService.state().nodes().getMasterNode();
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
            builder.remove(failedMasterNode.getId());
            builder.masterNodeId(null);

            setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));
        }

        action.doExecute(null, request, listener);
        awaitForkedTasks();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().getRoutingTable().allShards(new String[] { TEST_INDEX });
        Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shard : shardIt) {
            if (map.containsKey(shard.currentNodeId()) == false) {
                map.put(shard.currentNodeId(), new ArrayList<>());
            }
            map.get(shard.currentNodeId()).add(shard);
        }

        int totalShards = 0;
        int totalSuccessfulShards = 0;
        int totalFailedShards = 0;
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            List<BroadcastShardOperationFailedException> exceptions = new ArrayList<>();
            long requestId = entry.getValue().get(0).requestId();
            if (rarely()) {
                // simulate node failure
                totalShards += map.get(entry.getKey()).size();
                totalFailedShards += map.get(entry.getKey()).size();
                transport.handleRemoteError(requestId, new Exception());
            } else {
                List<ShardRouting> shards = map.get(entry.getKey());
                List<ShardResult> shardResults = new ArrayList<>();
                for (ShardRouting shard : shards) {
                    totalShards++;
                    if (rarely()) {
                        // simulate operation failure
                        totalFailedShards++;
                        exceptions.add(new BroadcastShardOperationFailedException(shard.shardId(), "operation indices:admin/test failed"));
                    } else {
                        shardResults.add(new ShardResult());
                    }
                }
                totalSuccessfulShards += shardResults.size();
                TransportBroadcastByNodeAction<Request, Response, ShardResult>.NodeResponse nodeResponse = action.new NodeResponse(
                    entry.getKey(), shards.size(), shardResults, exceptions
                );
                transport.handleResponse(requestId, nodeResponse);
            }
        }
        if (simulateFailedMasterNode) {
            totalShards += map.get(failedMasterNode.getId()).size();
        }

        Response response = listener.get();
        assertEquals("total shards", totalShards, response.getTotalShards());
        assertEquals("successful shards", totalSuccessfulShards, response.getSuccessfulShards());
        assertEquals("failed shards", totalFailedShards, response.getFailedShards());
        assertEquals("accumulated exceptions", totalFailedShards, response.getShardFailures().length);
    }

    public void testResponsesReleasedOnCancellation() {
        final CancellableTask cancellableTask = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());
        final PlainActionFuture<Response> listener = new PlainActionFuture<>();
        action.execute(cancellableTask, new Request(TEST_INDEX), listener);
        awaitForkedTasks();

        final List<CapturingTransport.CapturedRequest> capturedRequests = new ArrayList<>(
            Arrays.asList(transport.getCapturedRequestsAndClear())
        );
        Randomness.shuffle(capturedRequests);

        final ReachabilityChecker reachabilityChecker = new ReachabilityChecker();
        final Runnable nextRequestProcessor = () -> {
            final var capturedRequest = capturedRequests.remove(0);
            if (randomBoolean()) {
                // transport.handleResponse may de/serialize the response, releasing it early, so send the response straight to the handler
                transport.getTransportResponseHandler(capturedRequest.requestId())
                    .handleResponse(
                        action.new NodeResponse(
                            capturedRequest.node().getId(), 1, List.of(reachabilityChecker.register(new ShardResult())), List.of()
                        )
                    );
            } else {
                // handleRemoteError may de/serialize the exception, releasing it early, so just use handleLocalError
                transport.handleLocalError(
                    capturedRequest.requestId(),
                    reachabilityChecker.register(new ElasticsearchException("simulated"))
                );
            }
        };

        assertThat(capturedRequests.size(), greaterThan(2));
        final var responsesBeforeCancellation = between(1, capturedRequests.size() - 2);
        for (int i = 0; i < responsesBeforeCancellation; i++) {
            nextRequestProcessor.run();
        }

        reachabilityChecker.checkReachable();
        TaskCancelHelper.cancel(cancellableTask, "simulated");

        // responses captured before cancellation are now unreachable
        reachabilityChecker.ensureUnreachable();

        while (capturedRequests.size() > 0) {
            // a response sent after cancellation is dropped immediately
            assertFalse(listener.isDone());
            nextRequestProcessor.run();
            reachabilityChecker.ensureUnreachable();
        }

        expectThrows(TaskCancelledException.class, () -> listener.actionGet(10, TimeUnit.SECONDS));
    }

    public void testShardLevelOperationsStopOnCancellation() throws Exception {
        action = new TestTransportBroadcastByNodeAction("indices:admin/shard_level_test") {
            int expectedShardId;

            @Override
            protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<ShardResult> listener) {
                // this test runs a node-level operation on three shards, cancelling the task some time during the execution on the second
                if (task instanceof CancellableTask cancellableTask) {
                    assertEquals(expectedShardId++, shardRouting.shardId().id());
                    switch (shardRouting.shardId().id()) {
                        case 0 -> {
                            assertFalse(cancellableTask.isCancelled());
                            listener.onResponse(new ShardResult());
                        }
                        case 1 -> {
                            assertFalse(cancellableTask.isCancelled());
                            TaskCancelHelper.cancel(cancellableTask, "simulated");
                            if (randomBoolean()) {
                                listener.onResponse(new ShardResult());
                            } else {
                                assertTrue(cancellableTask.notifyIfCancelled(listener));
                            }
                        }
                        default -> fail("unexpected shard execution: " + shardRouting);
                    }
                } else {
                    fail("task was not cancellable");
                }
            }
        };

        final PlainActionFuture<TransportResponse> nodeResponseFuture = new PlainActionFuture<>();

        action.new BroadcastByNodeTransportRequestHandler().messageReceived(
            action.new NodeRequest(
                new Request(), IntStream.range(0, 3)
                    .mapToObj(shardId -> TestShardRouting.newShardRouting(TEST_INDEX, shardId, "node-id", true, ShardRoutingState.STARTED))
                    .toList(), "node-id"
            ),
            new TestTransportChannel(nodeResponseFuture),
            new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap())
        );

        assertTrue(nodeResponseFuture.isDone());
        assertEquals(
            "task cancelled [simulated]",
            expectThrows(
                java.util.concurrent.ExecutionException.class,
                org.elasticsearch.tasks.TaskCancelledException.class,
                nodeResponseFuture::get
            ).getMessage()
        );
    }

    public void testShardResultsReleasedOnCancellation() throws Exception {
        final var listeners = new ArrayList<ActionListener<ShardResult>>();

        action = new TestTransportBroadcastByNodeAction("indices:admin/shard_level_gc_test") {
            @Override
            protected void shardOperation(Request request, ShardRouting shardRouting, Task task, ActionListener<ShardResult> listener) {
                listeners.add(listener);
            }
        };

        final PlainActionFuture<TransportResponse> nodeResponseFuture = new PlainActionFuture<>();
        final CancellableTask task = new CancellableTask(randomLong(), "transport", "action", "", null, emptyMap());

        action.new BroadcastByNodeTransportRequestHandler().messageReceived(
            action.new NodeRequest(
                new Request(), IntStream.range(0, 3)
                    .mapToObj(shardId -> TestShardRouting.newShardRouting(TEST_INDEX, shardId, "node-id", true, ShardRoutingState.STARTED))
                    .toList(), "node-id"
            ),
            new TestTransportChannel(nodeResponseFuture),
            task
        );

        assertEquals(3, listeners.size());

        final var reachabilityChecker = new ReachabilityChecker();
        listeners.get(0).onResponse(reachabilityChecker.register(new ShardResult()));
        reachabilityChecker.checkReachable();

        TaskCancelHelper.cancel(task, "simulated");
        reachabilityChecker.ensureUnreachable();

        listeners.get(1).onResponse(reachabilityChecker.register(new ShardResult()));
        reachabilityChecker.ensureUnreachable();

        assertFalse(nodeResponseFuture.isDone());

        listeners.get(2).onResponse(reachabilityChecker.register(new ShardResult()));
        reachabilityChecker.ensureUnreachable();

        assertTrue(nodeResponseFuture.isDone());
        assertEquals(
            "task cancelled [simulated]",
            expectThrows(
                java.util.concurrent.ExecutionException.class,
                org.elasticsearch.tasks.TaskCancelledException.class,
                nodeResponseFuture::get
            ).getMessage()
        );
    }

}
