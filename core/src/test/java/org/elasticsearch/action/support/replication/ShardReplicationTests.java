/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.support.replication;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.TestClusterService;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithStartedPrimary;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShardReplicationTests extends ESTestCase {

    private static ThreadPool threadPool;

    private TestClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private Action action;
    /* *
    * TransportReplicationAction needs an instance of IndexShard to count operations.
    * indexShards is reset to null before each test and will be initialized upon request in the tests.
    */

    @BeforeClass
    public static void beforeClass() {
        threadPool = new ThreadPool("ShardReplicationTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = new TestClusterService(threadPool);
        transportService = new TransportService(transport, threadPool);
        transportService.start();
        action = new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool);
        count.set(1);
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    <T> void assertListenerThrows(String msg, PlainActionFuture<T> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

    @Test
    public void testBlocks() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        assertFalse("primary phase should stop execution", primaryPhase.checkBlocks());
        assertListenerThrows("primary phase should fail operation", listener, ClusterBlockException.class);

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "retryable", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(new Request().timeout("5ms"), listener);
        assertFalse("primary phase should stop execution on retryable block", primaryPhase.checkBlocks());
        assertListenerThrows("failed to timeout on retryable block", listener, ClusterBlockException.class);


        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(new Request(), listener);
        assertFalse("primary phase should stop execution on retryable block", primaryPhase.checkBlocks());
        assertFalse("primary phase should wait on retryable block", listener.isDone());

        block = ClusterBlocks.builder()
                .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        clusterService.setState(ClusterState.builder(clusterService.state()).blocks(block));
        assertListenerThrows("primary phase should fail operation when moving from a retryable block to a non-retryable one", listener, ClusterBlockException.class);
        assertIndexShardUninitialized();
    }

    public void assertIndexShardUninitialized() {
        assertEquals(1, count.get());
    }

    @Test
    public void testNotStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        // no replicas in oder to skip the replication part
        clusterService.setState(state(index, true,
                randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));

        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());

        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.run();
        assertListenerThrows("unassigned primary didn't cause a timeout", listener, UnavailableShardsException.class);

        request = new Request(shardId);
        listener = new PlainActionFuture<>();
        primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.run();
        assertFalse("unassigned primary didn't cause a retry", listener.isDone());

        clusterService.setState(state(index, true, ShardRoutingState.STARTED));
        logger.debug("--> primary assigned state:\n{}", clusterService.state().prettyPrint());

        listener.get();
        assertTrue("request wasn't processed on primary, despite of it being assigned", request.processedOnPrimary.get());
        assertIndexShardCounter(1);
    }

    @Test
    public void testRoutingToPrimary() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        clusterService.setState(stateWithStartedPrimary(index, randomBoolean(), 3));

        logger.debug("using state: \n{}", clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        Request request = new Request(shardId);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        assertTrue(primaryPhase.checkBlocks());
        primaryPhase.routeRequestOrPerformLocally(shardRoutingTable.primaryShard(), shardRoutingTable.shardsIt());
        if (primaryNodeId.equals(clusterService.localNode().id())) {
            logger.info("--> primary is assigned locally, testing for execution");
            assertTrue("request failed to be processed on a local primary", request.processedOnPrimary.get());
            if (transport.capturedRequests().length > 0) {
                assertIndexShardCounter(2);
            } else {
                assertIndexShardCounter(1);
            }
        } else {
            logger.info("--> primary is assigned to [{}], checking request forwarded", primaryNodeId);
            final List<CapturingTransport.CapturedRequest> capturedRequests = transport.capturedRequestsByTargetNode().get(primaryNodeId);
            assertThat(capturedRequests, notNullValue());
            assertThat(capturedRequests.size(), equalTo(1));
            assertThat(capturedRequests.get(0).action, equalTo("testAction"));
            assertIndexShardUninitialized();
        }
    }

    @Test
    public void testWriteConsistency() throws ExecutionException, InterruptedException {
        action = new ActionWithConsistency(Settings.EMPTY, "testActionWithConsistency", transportService, clusterService, threadPool);
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        final int assignedReplicas = randomInt(2);
        final int unassignedReplicas = randomInt(2);
        final int totalShards = 1 + assignedReplicas + unassignedReplicas;
        final boolean passesWriteConsistency;
        Request request = new Request(shardId).consistencyLevel(randomFrom(WriteConsistencyLevel.values()));
        switch (request.consistencyLevel()) {
            case ONE:
                passesWriteConsistency = true;
                break;
            case DEFAULT:
            case QUORUM:
                if (totalShards <= 2) {
                    passesWriteConsistency = true; // primary is enough
                } else {
                    passesWriteConsistency = assignedReplicas + 1 >= (totalShards / 2) + 1;
                }
                break;
            case ALL:
                passesWriteConsistency = unassignedReplicas == 0;
                break;
            default:
                throw new RuntimeException("unknown consistency level [" + request.consistencyLevel() + "]");
        }
        ShardRoutingState[] replicaStates = new ShardRoutingState[assignedReplicas + unassignedReplicas];
        for (int i = 0; i < assignedReplicas; i++) {
            replicaStates[i] = randomFrom(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
        }
        for (int i = assignedReplicas; i < replicaStates.length; i++) {
            replicaStates[i] = ShardRoutingState.UNASSIGNED;
        }

        clusterService.setState(state(index, true, ShardRoutingState.STARTED, replicaStates));
        logger.debug("using consistency level of [{}], assigned shards [{}], total shards [{}]. expecting op to [{}]. using state: \n{}",
                request.consistencyLevel(), 1 + assignedReplicas, 1 + assignedReplicas + unassignedReplicas, passesWriteConsistency ? "succeed" : "retry",
                clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        if (passesWriteConsistency) {
            assertThat(primaryPhase.checkWriteConsistency(shardRoutingTable.primaryShard()), nullValue());
            primaryPhase.run();
            assertTrue("operations should have been perform, consistency level is met", request.processedOnPrimary.get());
            if (assignedReplicas > 0) {
                assertIndexShardCounter(2);
            } else {
                assertIndexShardCounter(1);
            }
        } else {
            assertThat(primaryPhase.checkWriteConsistency(shardRoutingTable.primaryShard()), notNullValue());
            primaryPhase.run();
            assertFalse("operations should not have been perform, consistency level is *NOT* met", request.processedOnPrimary.get());
            assertIndexShardUninitialized();
            for (int i = 0; i < replicaStates.length; i++) {
                replicaStates[i] = ShardRoutingState.STARTED;
            }
            clusterService.setState(state(index, true, ShardRoutingState.STARTED, replicaStates));
            assertTrue("once the consistency level met, operation should continue", request.processedOnPrimary.get());
            assertIndexShardCounter(2);
        }
    }

    @Test
    public void testReplication() throws ExecutionException, InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        clusterService.setState(stateWithStartedPrimary(index, true, randomInt(5)));

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        int assignedReplicas = 0;
        int totalShards = 0;
        for (ShardRouting shard : shardRoutingTable) {
            totalShards++;
            if (shard.primary() == false && shard.assignedToNode()) {
                assignedReplicas++;
            }
            if (shard.relocating()) {
                assignedReplicas++;
                totalShards++;
            }
        }

        runReplicateTest(shardRoutingTable, assignedReplicas, totalShards);
    }

    @Test
    public void testReplicationWithShadowIndex() throws ExecutionException, InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);

        ClusterState state = stateWithStartedPrimary(index, true, randomInt(5));
        MetaData.Builder metaData = MetaData.builder(state.metaData());
        Settings.Builder settings = Settings.builder().put(metaData.get(index).settings());
        settings.put(IndexMetaData.SETTING_SHADOW_REPLICAS, true);
        metaData.put(IndexMetaData.builder(metaData.get(index)).settings(settings));
        clusterService.setState(ClusterState.builder(state).metaData(metaData));

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        int assignedReplicas = 0;
        int totalShards = 0;
        for (ShardRouting shard : shardRoutingTable) {
            totalShards++;
            if (shard.primary() && shard.relocating()) {
                assignedReplicas++;
                totalShards++;
            }
        }
        runReplicateTest(shardRoutingTable, assignedReplicas, totalShards);
    }


    protected void runReplicateTest(IndexShardRoutingTable shardRoutingTable, int assignedReplicas, int totalShards) throws InterruptedException, ExecutionException {
        final ShardRouting primaryShard = shardRoutingTable.primaryShard();
        final ShardIterator shardIt = shardRoutingTable.shardsIt();
        final ShardId shardId = shardIt.shardId();
        final Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        logger.debug("expecting [{}] assigned replicas, [{}] total shards. using state: \n{}", assignedReplicas, totalShards, clusterService.state().prettyPrint());

        final TransportReplicationAction<Request, Request, Response>.InternalRequest internalRequest = action.new InternalRequest(request);
        internalRequest.concreteIndex(shardId.index().name());
        Releasable reference = getOrCreateIndexShardOperationsCounter();
        assertIndexShardCounter(2);
        TransportReplicationAction<Request, Request, Response>.ReplicationPhase replicationPhase =
                action.new ReplicationPhase(shardIt, request,
                        new Response(), new ClusterStateObserver(clusterService, logger),
                        primaryShard, internalRequest, listener, reference);

        assertThat(replicationPhase.totalShards(), equalTo(totalShards));
        assertThat(replicationPhase.pending(), equalTo(assignedReplicas));
        replicationPhase.run();
        final CapturingTransport.CapturedRequest[] capturedRequests = transport.capturedRequests();
        transport.clear();
        assertThat(capturedRequests.length, equalTo(assignedReplicas));
        if (assignedReplicas > 0) {
            assertThat("listener is done, but there are outstanding replicas", listener.isDone(), equalTo(false));
        }
        int pending = replicationPhase.pending();
        int criticalFailures = 0; // failures that should fail the shard
        int successfull = 1;
        for (CapturingTransport.CapturedRequest capturedRequest : capturedRequests) {
            if (randomBoolean()) {
                Throwable t;
                if (randomBoolean()) {
                    t = new CorruptIndexException("simulated", (String) null);
                    criticalFailures++;
                } else {
                    t = new IndexShardNotStartedException(shardId, IndexShardState.RECOVERING);
                }
                logger.debug("--> simulating failure on {} with [{}]", capturedRequest.node, t.getClass().getSimpleName());
                transport.handleResponse(capturedRequest.requestId, t);
            } else {
                successfull++;
                transport.handleResponse(capturedRequest.requestId, TransportResponse.Empty.INSTANCE);
            }
            pending--;
            assertThat(replicationPhase.pending(), equalTo(pending));
            assertThat(replicationPhase.successful(), equalTo(successfull));
        }
        assertThat(listener.isDone(), equalTo(true));
        Response response = listener.get();
        final ActionWriteResponse.ShardInfo shardInfo = response.getShardInfo();
        assertThat(shardInfo.getFailed(), equalTo(criticalFailures));
        assertThat(shardInfo.getFailures(), arrayWithSize(criticalFailures));
        assertThat(shardInfo.getSuccessful(), equalTo(successfull));
        assertThat(shardInfo.getTotal(), equalTo(totalShards));

        assertThat("failed to see enough shard failures", transport.capturedRequests().length, equalTo(criticalFailures));
        for (CapturingTransport.CapturedRequest capturedRequest : transport.capturedRequests()) {
            assertThat(capturedRequest.action, equalTo(ShardStateAction.SHARD_FAILED_ACTION_NAME));
        }
        // all replicas have responded so the counter should be decreased again
        assertIndexShardCounter(1);
    }

    @Test
    public void testCounterOnPrimary() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        // no replica, we only want to test on primary
        clusterService.setState(state(index, true,
                ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(shardId).timeout("100ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        /**
         * Execute an action that is stuck in shard operation until a latch is counted down.
         * That way we can start the operation, check if the counter was incremented and then unblock the operation
         * again to see if the counter is decremented afterwards.
         * TODO: I could also write an action that asserts that the counter is 2 in the shard operation.
         * However, this failure would only become apparent once listener.get is called. Seems a little implicit.
         * */
        action = new ActionWithDelay(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, threadPool);
        final TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        Thread t = new Thread() {
            @Override
            public void run() {
                primaryPhase.run();
            }
        };
        t.start();
        // shard operation should be ongoing, so the counter is at 2
        // we have to wait here because increment happens in thread
        awaitBusy(() -> count.get() == 2);

        assertIndexShardCounter(2);
        assertThat(transport.capturedRequests().length, equalTo(0));
        ((ActionWithDelay) action).countDownLatch.countDown();
        t.join();
        listener.get();
        // operation finished, counter back to 0
        assertIndexShardCounter(1);
        assertThat(transport.capturedRequests().length, equalTo(0));
    }

    @Test
    public void testCounterIncrementedWhileReplicationOngoing() throws InterruptedException, ExecutionException, IOException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        // one replica to make sure replication is attempted
        clusterService.setState(state(index, true,
                ShardRoutingState.STARTED, ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(shardId).timeout("100ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.run();
        assertIndexShardCounter(2);
        assertThat(transport.capturedRequests().length, equalTo(1));
        // try once with successful response
        transport.handleResponse(transport.capturedRequests()[0].requestId, TransportResponse.Empty.INSTANCE);
        assertIndexShardCounter(1);
        transport.clear();
        request = new Request(shardId).timeout("100ms");
        primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.run();
        assertIndexShardCounter(2);
        assertThat(transport.capturedRequests().length, equalTo(1));
        // try with failure response
        transport.handleResponse(transport.capturedRequests()[0].requestId, new CorruptIndexException("simulated", (String) null));
        assertIndexShardCounter(1);
    }

    @Test
    public void testReplicasCounter() throws Exception {
        final ShardId shardId = new ShardId("test", 0);
        clusterService.setState(state(shardId.index().getName(), true,
                ShardRoutingState.STARTED, ShardRoutingState.STARTED));
        action = new ActionWithDelay(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, threadPool);
        final Action.ReplicaOperationTransportHandler replicaOperationTransportHandler = action.new ReplicaOperationTransportHandler();
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    replicaOperationTransportHandler.messageReceived(new Request(), createTransportChannel());
                } catch (Exception e) {
                }
            }
        };
        t.start();
        // shard operation should be ongoing, so the counter is at 2
        // we have to wait here because increment happens in thread
        awaitBusy(() -> count.get() == 2);
        ((ActionWithDelay) action).countDownLatch.countDown();
        t.join();
        // operation should have finished and counter decreased because no outstanding replica requests
        assertIndexShardCounter(1);
        // now check if this also works if operation throws exception
        action = new ActionWithExceptions(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, threadPool);
        final Action.ReplicaOperationTransportHandler replicaOperationTransportHandlerForException = action.new ReplicaOperationTransportHandler();
        try {
            replicaOperationTransportHandlerForException.messageReceived(new Request(shardId), createTransportChannel());
            fail();
        } catch (Throwable t2) {
        }
        assertIndexShardCounter(1);
    }

    @Test
    public void testCounterDecrementedIfShardOperationThrowsException() throws InterruptedException, ExecutionException, IOException {
        action = new ActionWithExceptions(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, threadPool);
        final String index = "test";
        final ShardId shardId = new ShardId(index, 0);
        clusterService.setState(state(index, true,
                ShardRoutingState.STARTED, ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(shardId).timeout("100ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        TransportReplicationAction<Request, Request, Response>.PrimaryPhase primaryPhase = action.new PrimaryPhase(request, listener);
        primaryPhase.run();
        // no replica request should have been sent yet
        assertThat(transport.capturedRequests().length, equalTo(0));
        // no matter if the operation is retried or not, counter must be be back to 1
        assertIndexShardCounter(1);
    }

    private void assertIndexShardCounter(int expected) {
        assertThat(count.get(), equalTo(expected));
    }

    private final AtomicInteger count = new AtomicInteger(0);

    /*
    * Returns testIndexShardOperationsCounter or initializes it if it was already created in this test run.
    * */
    private synchronized Releasable getOrCreateIndexShardOperationsCounter() {
        count.incrementAndGet();
        return new Releasable() {
            @Override
            public void close() {
                count.decrementAndGet();
            }
        };
    }

    public static class Request extends ReplicationRequest<Request> {
        int shardId;
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public AtomicInteger processedOnReplicas = new AtomicInteger();

        public Request() {
        }

        Request(ShardId shardId) {
            this();
            this.shardId = shardId.id();
            this.index(shardId.index().name());
            // keep things simple
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shardId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = in.readVInt();
        }
    }

    static class Response extends ActionWriteResponse {
    }

    class Action extends TransportReplicationAction<Request, Request, Response> {

        Action(Settings settings, String actionName, TransportService transportService,
               ClusterService clusterService,
               ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, null, threadPool,
                    new ShardStateAction(settings, clusterService, transportService, null, null), null,
                    new ActionFilters(new HashSet<ActionFilter>()), new IndexNameExpressionResolver(Settings.EMPTY), Request::new, Request::new, ThreadPool.Names.SAME);
        }

        @Override
        protected Response newResponseInstance() {
            return new Response();
        }

        @Override
        protected Tuple<Response, Request> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            boolean executedBefore = shardRequest.request.processedOnPrimary.getAndSet(true);
            assert executedBefore == false : "request has already been executed on the primary";
            return new Tuple<>(new Response(), shardRequest.request);
        }

        @Override
        protected void shardOperationOnReplica(ShardId shardId, Request request) {
            request.processedOnReplicas.incrementAndGet();
        }

        @Override
        protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
            return clusterState.getRoutingTable().index(request.concreteIndex()).shard(request.request().shardId).shardsIt();
        }

        @Override
        protected boolean checkWriteConsistency() {
            return false;
        }

        @Override
        protected boolean resolveIndex() {
            return false;
        }

        @Override
        protected Releasable getIndexShardOperationsCounter(ShardId shardId) {
            return getOrCreateIndexShardOperationsCounter();
        }
    }

    class ActionWithConsistency extends Action {

        ActionWithConsistency(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, threadPool);
        }

        @Override
        protected boolean checkWriteConsistency() {
            return true;
        }
    }

    /*
    * Throws exceptions when executed. Used for testing if the counter is correctly decremented in case an operation fails.
    * */
    class ActionWithExceptions extends Action {

        ActionWithExceptions(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) throws IOException {
            super(settings, actionName, transportService, clusterService, threadPool);
        }

        @Override
        protected Tuple<Response, Request> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            return throwException(shardRequest.shardId);
        }

        private Tuple<Response, Request> throwException(ShardId shardId) {
            try {
                if (randomBoolean()) {
                    // throw a generic exception
                    // for testing on replica this will actually cause an NPE because it will make the shard fail but
                    // for this we need an IndicesService which is null.
                    throw new ElasticsearchException("simulated");
                } else {
                    // throw an exception which will cause retry on primary and be ignored on replica
                    throw new IndexShardNotStartedException(shardId, IndexShardState.RECOVERING);
                }
            } catch (Exception e) {
                logger.info("throwing ", e);
                throw e;
            }
        }

        @Override
        protected void shardOperationOnReplica(ShardId shardId, Request shardRequest) {
            throwException(shardRequest.internalShardId);
        }
    }

    /**
     * Delays the operation until  countDownLatch is counted down
     */
    class ActionWithDelay extends Action {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        ActionWithDelay(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) throws IOException {
            super(settings, actionName, transportService, clusterService, threadPool);
        }

        @Override
        protected Tuple<Response, Request> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) throws Throwable {
            awaitLatch();
            return new Tuple<>(new Response(), shardRequest.request);
        }

        private void awaitLatch() throws InterruptedException {
            countDownLatch.await();
            countDownLatch = new CountDownLatch(1);
        }

        @Override
        protected void shardOperationOnReplica(ShardId shardId, Request shardRequest) {
            try {
                awaitLatch();
            } catch (InterruptedException e) {
            }
        }

    }

    /*
    * Transport channel that is needed for replica operation testing.
    * */
    public TransportChannel createTransportChannel() {
        return new TransportChannel() {

            @Override
            public String action() {
                return null;
            }

            @Override
            public String getProfileName() {
                return "";
            }

            @Override
            public void sendResponse(TransportResponse response) throws IOException {
            }

            @Override
            public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
            }

            @Override
            public void sendResponse(Throwable error) throws IOException {
            }
        };
    }

}
