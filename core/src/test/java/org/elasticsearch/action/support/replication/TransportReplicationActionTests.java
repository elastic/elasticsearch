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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.EngineClosedException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESAllocationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportReplicationActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private Action action;
    /* *
    * TransportReplicationAction needs an instance of IndexShard to count operations.
    * indexShards is reset to null before each test and will be initialized upon request in the tests.
    */

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = new TransportService(clusterService.getSettings(), transport, threadPool);
        transportService.start();
        transportService.acceptIncomingRequests();
        action = new Action(Settings.EMPTY, "testAction", transportService, clusterService, threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
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

    public void testBlocks() throws ExecutionException, InterruptedException {
        Request request = new Request();
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        Action.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("primary phase should fail operation", listener, ClusterBlockException.class);
        assertPhase(task, "failed");

        block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "retryable", true, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, new Request().timeout("5ms"), listener);
        reroutePhase.run();
        assertListenerThrows("failed to timeout on retryable block", listener, ClusterBlockException.class);
        assertPhase(task, "failed");

        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, new Request(), listener);
        reroutePhase.run();
        assertFalse("primary phase should wait on retryable block", listener.isDone());
        assertPhase(task, "waiting_for_retry");

        block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        assertListenerThrows("primary phase should fail operation when moving from a retryable block to a non-retryable one", listener,
            ClusterBlockException.class);
        assertIndexShardUninitialized();
    }

    public void assertIndexShardUninitialized() {
        assertEquals(0, count.get());
    }

    public void testNotStartedPrimary() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replicas in oder to skip the replication part
        setState(clusterService, state(index, true,
            randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));
        ReplicationTask task = maybeTask();

        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());

        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        Action.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("unassigned primary didn't cause a timeout", listener, UnavailableShardsException.class);
        assertPhase(task, "failed");

        request = new Request(shardId);
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertFalse("unassigned primary didn't cause a retry", listener.isDone());
        assertPhase(task, "waiting_for_retry");

        setState(clusterService, state(index, true, ShardRoutingState.STARTED));
        logger.debug("--> primary assigned state:\n{}", clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        final List<CapturingTransport.CapturedRequest> capturedRequests =
            transport.getCapturedRequestsByTargetNodeAndClear().get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        assertThat(capturedRequests.get(0).action, equalTo("testAction[p]"));
        assertIndexShardCounter(0);
    }

    /**
     * When relocating a primary shard, there is a cluster state update at the end of relocation where the active primary is switched from
     * the relocation source to the relocation target. If relocation source receives and processes this cluster state
     * before the relocation target, there is a time span where relocation source believes active primary to be on
     * relocation target and relocation target believes active primary to be on relocation source. This results in replication
     * requests being sent back and forth.
     * <p>
     * This test checks that replication request is not routed back from relocation target to relocation source in case of
     * stale index routing table on relocation target.
     */
    public void testNoRerouteOnStaleClusterState() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = state(index, true, ShardRoutingState.RELOCATING);
        String relocationTargetNode = state.getRoutingTable().shardRoutingTable(shardId).primaryShard().relocatingNodeId();
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(relocationTargetNode)).build();
        setState(clusterService, state);
        logger.debug("--> relocation ongoing state:\n{}", clusterService.state().prettyPrint());

        Request request = new Request(shardId).timeout("1ms").routedBasedOnClusterVersion(clusterService.state().version() + 1);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        Action.ReroutePhase reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertListenerThrows("cluster state too old didn't cause a timeout", listener, UnavailableShardsException.class);

        request = new Request(shardId).routedBasedOnClusterVersion(clusterService.state().version() + 1);
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertFalse("cluster state too old didn't cause a retry", listener.isDone());

        // finish relocation
        ShardRouting relocationTarget = clusterService.state().getRoutingTable().shardRoutingTable(shardId)
            .shardsWithState(ShardRoutingState.INITIALIZING).get(0);
        AllocationService allocationService = ESAllocationTestCase.createAllocationService();
        RoutingAllocation.Result result = allocationService.applyStartedShards(state, Arrays.asList(relocationTarget));
        ClusterState updatedState = ClusterState.builder(clusterService.state()).routingResult(result).build();

        setState(clusterService, updatedState);
        logger.debug("--> relocation complete state:\n{}", clusterService.state().prettyPrint());

        IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        final List<CapturingTransport.CapturedRequest> capturedRequests =
            transport.getCapturedRequestsByTargetNodeAndClear().get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        assertThat(capturedRequests.get(0).action, equalTo("testAction[p]"));
        assertIndexShardCounter(0);
    }

    public void testUnknownIndexOrShardOnReroute() throws InterruptedException {
        final String index = "test";
        // no replicas in oder to skip the replication part
        setState(clusterService, state(index, true,
            randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(new ShardId("unknown_index", "_na_", 0)).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        Action.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw index not found exception", listener, IndexNotFoundException.class);
        assertPhase(task, "failed");
        request = new Request(new ShardId(index, "_na_", 10)).timeout("1ms");
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw shard not found exception", listener, ShardNotFoundException.class);
    }

    public void testStalePrimaryShardOnReroute() throws InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replicas in order to skip the replication part
        setState(clusterService, stateWithActivePrimary(index, true, randomInt(3)));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(shardId);
        boolean timeout = randomBoolean();
        if (timeout) {
            request.timeout("0s");
        } else {
            request.timeout("1h");
        }
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        Action.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests, arrayWithSize(1));
        assertThat(capturedRequests[0].action, equalTo("testAction[p]"));
        assertPhase(task, "waiting_on_primary");
        transport.handleRemoteError(capturedRequests[0].requestId, randomRetryPrimaryException(shardId));


        if (timeout) {
            // we always try at least one more time on timeout
            assertThat(listener.isDone(), equalTo(false));
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests, arrayWithSize(1));
            assertThat(capturedRequests[0].action, equalTo("testAction[p]"));
            assertPhase(task, "waiting_on_primary");
            transport.handleRemoteError(capturedRequests[0].requestId, randomRetryPrimaryException(shardId));
            assertListenerThrows("must throw index not found exception", listener, ElasticsearchException.class);
            assertPhase(task, "failed");
        } else {
            assertThat(listener.isDone(), equalTo(false));
            // generate a CS change
            setState(clusterService, clusterService.state());
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests, arrayWithSize(1));
            assertThat(capturedRequests[0].action, equalTo("testAction[p]"));
        }
    }

    private ElasticsearchException randomRetryPrimaryException(ShardId shardId) {
        return randomFrom(
            new ShardNotFoundException(shardId),
            new IndexNotFoundException(shardId.getIndex()),
            new IndexShardClosedException(shardId),
            new EngineClosedException(shardId),
            new ReplicationOperation.RetryOnPrimaryException(shardId, "hello")
        );
    }

    public void testRoutePhaseExecutesRequest() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ReplicationTask task = maybeTask();

        setState(clusterService, stateWithActivePrimary(index, randomBoolean(), 3));
        logger.debug("using state: \n{}", clusterService.state().prettyPrint());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        Request request = new Request(shardId);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        Action.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertThat(request.shardId(), equalTo(shardId));
        logger.info("--> primary is assigned to [{}], checking request forwarded", primaryNodeId);
        final List<CapturingTransport.CapturedRequest> capturedRequests =
            transport.getCapturedRequestsByTargetNodeAndClear().get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        if (clusterService.state().nodes().getLocalNodeId().equals(primaryNodeId)) {
            assertThat(capturedRequests.get(0).action, equalTo("testAction[p]"));
            assertPhase(task, "waiting_on_primary");
        } else {
            assertThat(capturedRequests.get(0).action, equalTo("testAction"));
            assertPhase(task, "rerouted");
        }
        assertIndexShardUninitialized();
    }

    public void testPrimaryPhaseExecutesOrDelegatesRequestToRelocationTarget() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = stateWithActivePrimary(index, true, randomInt(5));
        setState(clusterService, state);
        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        AtomicBoolean executed = new AtomicBoolean();
        Action.PrimaryOperationTransportHandler primaryPhase = action.new PrimaryOperationTransportHandler() {
            @Override
            protected ReplicationOperation<Request, Request, Action.PrimaryResult> createReplicatedOperation(Request request,
                    ActionListener<Action.PrimaryResult> actionListener, Action.PrimaryShardReference primaryShardReference,
                    boolean executeOnReplicas) {
                return new NoopReplicationOperation(request, actionListener) {
                    public void execute() throws Exception {
                        assertPhase(task, "primary");
                        assertFalse(executed.getAndSet(true));
                        super.execute();
                    }
                };
            }
        };
        ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        boolean executeOnPrimary = true;
        // whether shard has been marked as relocated already (i.e. relocation completed)
        if (primaryShard.relocating() && randomBoolean()) {
            isRelocated.set(true);
            executeOnPrimary = false;
        }
        primaryPhase.messageReceived(request, createTransportChannel(listener), task);
        if (executeOnPrimary) {
            assertTrue(executed.get());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
        } else {
            assertFalse(executed.get());
            assertIndexShardCounter(0);  // it should have been freed.
            final List<CapturingTransport.CapturedRequest> requests =
                transport.capturedRequestsByTargetNode().get(primaryShard.relocatingNodeId());
            assertThat(requests, notNullValue());
            assertThat(requests.size(), equalTo(1));
            assertThat("primary request was not delegated to relocation target", requests.get(0).action, equalTo("testAction[p]"));
            assertPhase(task, "primary_delegation");
            transport.handleResponse(requests.get(0).requestId, new Response());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
        }
    }

    public void testPrimaryPhaseExecutesDelegatedRequestOnRelocationTarget() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = state(index, true, ShardRoutingState.RELOCATING);
        String primaryTargetNodeId = state.getRoutingTable().shardRoutingTable(shardId).primaryShard().relocatingNodeId();
        // simulate execution of the primary phase on the relocation target node
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(primaryTargetNodeId)).build();
        setState(clusterService, state);
        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        AtomicBoolean executed = new AtomicBoolean();
        Action.PrimaryOperationTransportHandler primaryPhase = action.new PrimaryOperationTransportHandler() {
            @Override
            protected ReplicationOperation<Request, Request, Action.PrimaryResult> createReplicatedOperation(Request request,
                    ActionListener<Action.PrimaryResult> actionListener, Action.PrimaryShardReference primaryShardReference,
                    boolean executeOnReplicas) {
                return new NoopReplicationOperation(request, actionListener) {
                    public void execute() throws Exception {
                        assertPhase(task, "primary");
                        assertFalse(executed.getAndSet(true));
                        super.execute();
                    }
                };
            }
        };
        primaryPhase.messageReceived(request, createTransportChannel(listener), task);
        assertThat(executed.get(), equalTo(true));
        assertPhase(task, "finished");
    }

    public void testPrimaryReference() throws Exception {
        final IndexShard shard = mock(IndexShard.class);
        final long primaryTerm = 1 + randomInt(200);
        when(shard.getPrimaryTerm()).thenReturn(primaryTerm);

        AtomicBoolean closed = new AtomicBoolean();
        Releasable releasable = () -> {
            if (closed.compareAndSet(false, true) == false) {
                fail("releasable is closed twice");
            }
        };
        Action.PrimaryShardReference primary = action.new PrimaryShardReference(shard, releasable);
        final Request request = new Request();
        Request replicaRequest = primary.perform(request).replicaRequest;

        assertThat(replicaRequest.primaryTerm(), equalTo(primaryTerm));

        final ElasticsearchException exception = new ElasticsearchException("testing");
        primary.failShard("test", exception);

        verify(shard).failShard("test", exception);

        primary.close();

        assertTrue(closed.get());
    }

    public void testReplicaProxy() throws InterruptedException, ExecutionException {
        Action.ReplicasProxy proxy = action.new ReplicasProxy();
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = stateWithActivePrimary(index, true, 1 + randomInt(3), randomInt(2));
        logger.info("using state: {}", state.prettyPrint());
        setState(clusterService, state);

        // check that at unknown node fails
        PlainActionFuture<TransportResponse.Empty> listener = new PlainActionFuture<>();
        proxy.performOn(
            TestShardRouting.newShardRouting(shardId, "NOT THERE", false, randomFrom(ShardRoutingState.values())),
            new Request(), listener);
        assertTrue(listener.isDone());
        assertListenerThrows("non existent node should throw a NoNodeAvailableException", listener, NoNodeAvailableException.class);

        final IndexShardRoutingTable shardRoutings = state.routingTable().shardRoutingTable(shardId);
        final ShardRouting replica = randomFrom(shardRoutings.replicaShards().stream()
            .filter(ShardRouting::assignedToNode).collect(Collectors.toList()));
        listener = new PlainActionFuture<>();
        proxy.performOn(replica, new Request(), listener);
        assertFalse(listener.isDone());

        CapturingTransport.CapturedRequest[] captures = transport.getCapturedRequestsAndClear();
        assertThat(captures, arrayWithSize(1));
        if (randomBoolean()) {
            transport.handleResponse(captures[0].requestId, TransportResponse.Empty.INSTANCE);
            assertTrue(listener.isDone());
            listener.get();
        } else if (randomBoolean()) {
            transport.handleRemoteError(captures[0].requestId, new ElasticsearchException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, ElasticsearchException.class);
        } else {
            transport.handleError(captures[0].requestId, new TransportException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, TransportException.class);
        }

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Throwable> ignoredFailure = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();
        proxy.failShard(replica, shardRoutings.primaryShard(), "test", new ElasticsearchException("simulated"),
            () -> success.set(true), failure::set, ignoredFailure::set
        );
        CapturingTransport.CapturedRequest[] shardFailedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, shardFailedRequests.length);
        CapturingTransport.CapturedRequest shardFailedRequest = shardFailedRequests[0];
        ShardStateAction.ShardRoutingEntry shardRoutingEntry = (ShardStateAction.ShardRoutingEntry) shardFailedRequest.request;
        // the shard the request was sent to and the shard to be failed should be the same
        assertEquals(shardRoutingEntry.getShardRouting(), replica);
        if (randomBoolean()) {
            // simulate success
            transport.handleResponse(shardFailedRequest.requestId, TransportResponse.Empty.INSTANCE);
            assertTrue(success.get());
            assertNull(failure.get());
            assertNull(ignoredFailure.get());

        } else if (randomBoolean()) {
            // simulate the primary has been demoted
            transport.handleRemoteError(shardFailedRequest.requestId,
                new ShardStateAction.NoLongerPrimaryShardException(shardRoutingEntry.getShardRouting().shardId(),
                    "shard-failed-test"));
            assertFalse(success.get());
            assertNotNull(failure.get());
            assertNull(ignoredFailure.get());

        } else {
            // simulated an "ignored" exception
            transport.handleRemoteError(shardFailedRequest.requestId,
                new NodeClosedException(state.nodes().getLocalNode()));
            assertFalse(success.get());
            assertNull(failure.get());
            assertNotNull(ignoredFailure.get());
        }
    }

    public void testShadowIndexDisablesReplication() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);

        ClusterState state = stateWithActivePrimary(index, true, randomInt(5));
        MetaData.Builder metaData = MetaData.builder(state.metaData());
        Settings.Builder settings = Settings.builder().put(metaData.get(index).getSettings());
        settings.put(IndexMetaData.SETTING_SHADOW_REPLICAS, true);
        metaData.put(IndexMetaData.builder(metaData.get(index)).settings(settings));
        state = ClusterState.builder(state).metaData(metaData).build();
        setState(clusterService, state);
        Action.PrimaryOperationTransportHandler primaryPhase = action.new PrimaryOperationTransportHandler() {
            @Override
            protected ReplicationOperation<Request, Request, Action.PrimaryResult> createReplicatedOperation(Request request,
                    ActionListener<Action.PrimaryResult> actionListener, Action.PrimaryShardReference primaryShardReference,
                    boolean executeOnReplicas) {
                assertFalse(executeOnReplicas);
                return new NoopReplicationOperation(request, actionListener);
            }
        };
        primaryPhase.messageReceived(new Request(shardId), createTransportChannel(new PlainActionFuture<>()), null);
    }

    public void testCounterOnPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replica, we only want to test on primary
        setState(clusterService, state(index, true, ShardRoutingState.STARTED));
        logger.debug("--> using initial state:\n{}", clusterService.state().prettyPrint());
        Request request = new Request(shardId);
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        int i = randomInt(3);
        final boolean throwExceptionOnCreation = i == 1;
        final boolean throwExceptionOnRun = i == 2;
        final boolean respondWithError = i == 3;
        Action.PrimaryOperationTransportHandler primaryPhase = action.new PrimaryOperationTransportHandler() {

            @Override
            protected ReplicationOperation<Request, Request, Action.PrimaryResult> createReplicatedOperation(Request request,
                    ActionListener<Action.PrimaryResult> listener, Action.PrimaryShardReference primaryShardReference,
                    boolean executeOnReplicas) {
                assertIndexShardCounter(1);
                if (throwExceptionOnCreation) {
                    throw new ElasticsearchException("simulated exception, during createReplicatedOperation");
                }
                return new NoopReplicationOperation(request, listener) {
                    @Override
                    public void execute() throws Exception {
                        assertIndexShardCounter(1);
                        assertPhase(task, "primary");
                        if (throwExceptionOnRun) {
                            throw new ElasticsearchException("simulated exception, during performOnPrimary");
                        } else if (respondWithError) {
                            this.resultListener.onFailure(new ElasticsearchException("simulated exception, as a response"));
                        } else {
                            super.execute();
                        }
                    }
                };
            }
        };
        try {
            primaryPhase.messageReceived(request, createTransportChannel(listener), task);
        } catch (ElasticsearchException e) {
            if (throwExceptionOnCreation || throwExceptionOnRun) {
                assertThat(e.getMessage(), containsString("simulated"));
                assertIndexShardCounter(0);
                return; // early terminate
            } else {
                throw e;
            }
        }
        assertIndexShardCounter(0);
        assertTrue(listener.isDone());
        assertPhase(task, "finished");

        try {
            listener.get();
        } catch (ExecutionException e) {
            if (respondWithError) {
                Throwable cause = e.getCause();
                assertThat(cause, instanceOf(ElasticsearchException.class));
                assertThat(cause.getMessage(), containsString("simulated"));
            } else {
                throw e;
            }
        }
    }

    public void testReplicasCounter() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        setState(clusterService, state(shardId.getIndexName(), true,
            ShardRoutingState.STARTED, ShardRoutingState.STARTED));
        boolean throwException = randomBoolean();
        final ReplicationTask task = maybeTask();
        Action action = new Action(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, threadPool) {
            @Override
            protected ReplicaResult shardOperationOnReplica(Request request) {
                assertIndexShardCounter(1);
                assertPhase(task, "replica");
                if (throwException) {
                    throw new ElasticsearchException("simulated");
                }
                return new ReplicaResult();
            }
        };
        final Action.ReplicaOperationTransportHandler replicaOperationTransportHandler = action.new ReplicaOperationTransportHandler();
        try {
            replicaOperationTransportHandler.messageReceived(new Request().setShardId(shardId),
                createTransportChannel(new PlainActionFuture<>()), task);
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("simulated"));
            assertTrue(throwException);
        }
        assertPhase(task, "finished");
        // operation should have finished and counter decreased because no outstanding replica requests
        assertIndexShardCounter(0);
    }

    private void assertIndexShardCounter(int expected) {
        assertThat(count.get(), equalTo(expected));
    }

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicBoolean isRelocated = new AtomicBoolean(false);

    /**
     * Sometimes build a ReplicationTask for tracking the phase of the
     * TransportReplicationAction. Since TransportReplicationAction has to work
     * if the task as null just as well as if it is supplied this returns null
     * half the time.
     */
    private ReplicationTask maybeTask() {
        return random().nextBoolean() ? new ReplicationTask(0, null, null, null, null) : null;
    }

    /**
     * If the task is non-null this asserts that the phrase matches.
     */
    private void assertPhase(@Nullable ReplicationTask task, String phase) {
        assertPhase(task, equalTo(phase));
    }

    private void assertPhase(@Nullable ReplicationTask task, Matcher<String> phaseMatcher) {
        if (task != null) {
            assertThat(task.getPhase(), phaseMatcher);
        }
    }

    public static class Request extends ReplicationRequest<Request> {
        public AtomicBoolean processedOnPrimary = new AtomicBoolean();
        public AtomicInteger processedOnReplicas = new AtomicInteger();

        public Request() {
        }

        Request(ShardId shardId) {
            this();
            this.shardId = shardId;
            this.index = shardId.getIndexName();
            // keep things simple
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }
    }

    static class Response extends ReplicationResponse {
    }

    class Action extends TransportReplicationAction<Request, Request, Response> {

        Action(Settings settings, String actionName, TransportService transportService,
               ClusterService clusterService,
               ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, null, threadPool,
                new ShardStateAction(settings, clusterService, transportService, null, null, threadPool),
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                Request::new, Request::new, ThreadPool.Names.SAME);
        }

        @Override
        protected Response newResponseInstance() {
            return new Response();
        }

        @Override
        protected PrimaryResult shardOperationOnPrimary(Request shardRequest) throws Exception {
            boolean executedBefore = shardRequest.processedOnPrimary.getAndSet(true);
            assert executedBefore == false : "request has already been executed on the primary";
            return new PrimaryResult(shardRequest, new Response());
        }

        @Override
        protected ReplicaResult shardOperationOnReplica(Request request) {
            request.processedOnReplicas.incrementAndGet();
            return new ReplicaResult();
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
        protected PrimaryShardReference getPrimaryShardReference(ShardId shardId) {
            count.incrementAndGet();
            return new PrimaryShardReference(null, null) {
                @Override
                public boolean isRelocated() {
                    return isRelocated.get();
                }

                @Override
                public void failShard(String reason, @Nullable Throwable e) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ShardRouting routingEntry() {
                    ShardRouting shardRouting = clusterService.state().getRoutingTable()
                        .shardRoutingTable(shardId).primaryShard();
                    assert shardRouting != null;
                    return shardRouting;
                }

                @Override
                public void close() {
                    count.decrementAndGet();
                }

            };
        }

        protected Releasable acquireReplicaOperationLock(ShardId shardId, long primaryTerm) {
            count.incrementAndGet();
            return count::decrementAndGet;
        }
    }

    class NoopReplicationOperation extends ReplicationOperation<Request, Request, Action.PrimaryResult> {
        public NoopReplicationOperation(Request request, ActionListener<Action.PrimaryResult> listener) {
            super(request, null, listener, true, true, null, null, TransportReplicationActionTests.this.logger, "noop");
        }

        @Override
        public void execute() throws Exception {
            this.resultListener.onResponse(action.new PrimaryResult(null, new Response()));
        }
    }

    /**
     * Transport channel that is needed for replica operation testing.
     */
    public TransportChannel createTransportChannel(final PlainActionFuture<Response> listener) {
        return createTransportChannel(listener, error -> {
        });
    }

    public TransportChannel createTransportChannel(final PlainActionFuture<Response> listener, Consumer<Throwable> consumer) {
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
                listener.onResponse(((Response) response));
            }

            @Override
            public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
                listener.onResponse(((Response) response));
            }

            @Override
            public void sendResponse(Throwable error) throws IOException {
                consumer.accept(error);
                listener.onFailure(error);
            }

            @Override
            public long getRequestId() {
                return 0;
            }

            @Override
            public String getChannelType() {
                return "replica_test";
            }
        };
    }

}
