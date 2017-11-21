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

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.cluster.ClusterStateChanges;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportReplicationActionTests extends ESTestCase {

    /**
     * takes a request that was sent by a {@link TransportReplicationAction} and captured
     * and returns the underlying request if it's wrapped or the original (cast to the expected type).
     *
     * This will throw a {@link ClassCastException} if the request is of the wrong type.
     */
    public static <R extends ReplicationRequest> R resolveRequest(TransportRequest requestOrWrappedRequest) {
        if (requestOrWrappedRequest instanceof TransportReplicationAction.ConcreteShardRequest) {
            requestOrWrappedRequest = ((TransportReplicationAction.ConcreteShardRequest<?>)requestOrWrappedRequest).getRequest();
        }
        return (R) requestOrWrappedRequest;
    }

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private TestAction action;
    private ShardStateAction shardStateAction;

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
        transportService = new TransportService(clusterService.getSettings(), transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> clusterService.localNode(), null);
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(Settings.EMPTY, clusterService, transportService, null, null, threadPool);
        action = new TestAction(Settings.EMPTY, "testAction", transportService, clusterService, shardStateAction, threadPool);
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
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        TestAction action = new TestAction(Settings.EMPTY, "testActionWithBlocks",
                transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected ClusterBlockLevel globalBlockLevel() {
                return ClusterBlockLevel.WRITE;
            }
        };

        ClusterBlocks.Builder block = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(1, "non retryable", false, true,
            false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("primary phase should fail operation", listener, ClusterBlockException.class);
        assertPhase(task, "failed");

        block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "retryable", true, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, new Request().timeout("5ms"), listener);
        reroutePhase.run();
        assertListenerThrows("failed to timeout on retryable block", listener, ClusterBlockException.class);
        assertPhase(task, "failed");
        assertFalse(request.isRetrySet.get());

        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, request = new Request(), listener);
        reroutePhase.run();
        assertFalse("primary phase should wait on retryable block", listener.isDone());
        assertPhase(task, "waiting_for_retry");
        assertTrue(request.isRetrySet.get());

        block = ClusterBlocks.builder().addGlobalBlock(new ClusterBlock(1, "non retryable", false, true, false,
            RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        assertListenerThrows("primary phase should fail operation when moving from a retryable block to a non-retryable one", listener,
            ClusterBlockException.class);
        assertIndexShardUninitialized();

        action = new TestAction(Settings.EMPTY, "testActionWithNoBlocks", transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected ClusterBlockLevel globalBlockLevel() {
                return null;
            }
        };
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, new Request().timeout("5ms"), listener);
        reroutePhase.run();
        assertListenerThrows("should fail with an IndexNotFoundException when no blocks checked", listener, IndexNotFoundException.class);
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

        logger.debug("--> using initial state:\n{}", clusterService.state());

        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("unassigned primary didn't cause a timeout", listener, UnavailableShardsException.class);
        assertPhase(task, "failed");
        assertTrue(request.isRetrySet.get());

        request = new Request(shardId);
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertFalse("unassigned primary didn't cause a retry", listener.isDone());
        assertPhase(task, "waiting_for_retry");
        assertTrue(request.isRetrySet.get());

        setState(clusterService, state(index, true, ShardRoutingState.STARTED));
        logger.debug("--> primary assigned state:\n{}", clusterService.state());

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
        logger.debug("--> relocation ongoing state:\n{}", clusterService.state());

        Request request = new Request(shardId).timeout("1ms").routedBasedOnClusterVersion(clusterService.state().version() + 1);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertListenerThrows("cluster state too old didn't cause a timeout", listener, UnavailableShardsException.class);
        assertTrue(request.isRetrySet.compareAndSet(true, false));

        request = new Request(shardId).routedBasedOnClusterVersion(clusterService.state().version() + 1);
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertFalse("cluster state too old didn't cause a retry", listener.isDone());
        assertTrue(request.isRetrySet.get());

        // finish relocation
        ShardRouting relocationTarget = clusterService.state().getRoutingTable().shardRoutingTable(shardId)
            .shardsWithState(ShardRoutingState.INITIALIZING).get(0);
        AllocationService allocationService = ESAllocationTestCase.createAllocationService();
        ClusterState updatedState = allocationService.applyStartedShards(state, Collections.singletonList(relocationTarget));

        setState(clusterService, updatedState);
        logger.debug("--> relocation complete state:\n{}", clusterService.state());

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
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Request request = new Request(new ShardId("unknown_index", "_na_", 0)).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw index not found exception", listener, IndexNotFoundException.class);
        assertPhase(task, "failed");
        assertTrue(request.isRetrySet.get());
        request = new Request(new ShardId(index, "_na_", 10)).timeout("1ms");
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw shard not found exception", listener, ShardNotFoundException.class);
        assertFalse(request.isRetrySet.get()); //TODO I'd have expected this to be true but we fail too early?

    }

    public void testClosedIndexOnReroute() throws InterruptedException {
        final String index = "test";
        // no replicas in oder to skip the replication part
        setState(clusterService, new ClusterStateChanges(xContentRegistry(), threadPool).closeIndices(state(index, true,
            ShardRoutingState.UNASSIGNED), new CloseIndexRequest(index)));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Request request = new Request(new ShardId("test", "_na_", 0)).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        ClusterBlockLevel indexBlockLevel = randomBoolean() ? ClusterBlockLevel.WRITE : null;
        TestAction action = new TestAction(Settings.EMPTY, "testActionWithBlocks", transportService,
                clusterService, shardStateAction, threadPool) {
            @Override
            protected ClusterBlockLevel indexBlockLevel() {
                return indexBlockLevel;
            }
        };
        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        if (indexBlockLevel == ClusterBlockLevel.WRITE) {
            assertListenerThrows("must throw block exception", listener, ClusterBlockException.class);
        } else {
            assertListenerThrows("must throw index closed exception", listener, IndexClosedException.class);
        }
        assertPhase(task, "failed");
        assertFalse(request.isRetrySet.get());
    }

    public void testStalePrimaryShardOnReroute() throws InterruptedException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replicas in order to skip the replication part
        setState(clusterService, stateWithActivePrimary(index, true, randomInt(3)));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Request request = new Request(shardId);
        boolean timeout = randomBoolean();
        if (timeout) {
            request.timeout("0s");
        } else {
            request.timeout("1h");
        }
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        CapturingTransport.CapturedRequest[] capturedRequests = transport.getCapturedRequestsAndClear();
        assertThat(capturedRequests, arrayWithSize(1));
        assertThat(capturedRequests[0].action, equalTo("testAction[p]"));
        assertPhase(task, "waiting_on_primary");
        assertFalse(request.isRetrySet.get());
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

    private Exception randomRetryPrimaryException(ShardId shardId) {
        return randomFrom(
            new ShardNotFoundException(shardId),
            new IndexNotFoundException(shardId.getIndex()),
            new IndexShardClosedException(shardId),
            new AlreadyClosedException(shardId + " primary is closed"),
            new ReplicationOperation.RetryOnPrimaryException(shardId, "hello")
        );
    }

    public void testRoutePhaseExecutesRequest() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ReplicationTask task = maybeTask();

        setState(clusterService, stateWithActivePrimary(index, randomBoolean(), 3));
        logger.debug("using state: \n{}", clusterService.state());

        final IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        Request request = new Request(shardId);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();

        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
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
        assertFalse(request.isRetrySet.get());
        assertIndexShardUninitialized();
    }

    public void testPrimaryPhaseExecutesOrDelegatesRequestToRelocationTarget() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = stateWithActivePrimary(index, true, randomInt(5));
        setState(clusterService, state);
        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        AtomicBoolean executed = new AtomicBoolean();

        ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metaData().index(index).primaryTerm(shardId.id());
        boolean executeOnPrimary = true;
        // whether shard has been marked as relocated already (i.e. relocation completed)
        if (primaryShard.relocating() && randomBoolean()) {
            isRelocated.set(true);
            executeOnPrimary = false;
        }
        action.new AsyncPrimaryAction(request, primaryShard.allocationId().getId(), primaryTerm, createTransportChannel(listener), task) {
            @Override
            protected ReplicationOperation<Request, Request, TransportReplicationAction.PrimaryResult<Request, TestResponse>>
            createReplicatedOperation(
                    Request request,
                    ActionListener<TransportReplicationAction.PrimaryResult<Request, TestResponse>> actionListener,
                    TransportReplicationAction<Request, Request, TestResponse>.PrimaryShardReference primaryShardReference) {
                return new NoopReplicationOperation(request, actionListener) {
                    public void execute() throws Exception {
                        assertPhase(task, "primary");
                        assertFalse(executed.getAndSet(true));
                        super.execute();
                    }
                };
            }
        }.run();
        if (executeOnPrimary) {
            assertTrue(executed.get());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
            assertFalse(request.isRetrySet.get());
        } else {
            assertFalse(executed.get());
            assertIndexShardCounter(0);  // it should have been freed.
            final List<CapturingTransport.CapturedRequest> requests =
                transport.capturedRequestsByTargetNode().get(primaryShard.relocatingNodeId());
            assertThat(requests, notNullValue());
            assertThat(requests.size(), equalTo(1));
            assertThat("primary request was not delegated to relocation target", requests.get(0).action, equalTo("testAction[p]"));
            assertThat("primary term not properly set on primary delegation",
                ((TransportReplicationAction.ConcreteShardRequest<Request>)requests.get(0).request).getPrimaryTerm(), equalTo(primaryTerm));
            assertPhase(task, "primary_delegation");
            transport.handleResponse(requests.get(0).requestId, new TestResponse());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
            assertFalse(request.isRetrySet.get());
        }
    }

    public void testPrimaryPhaseExecutesDelegatedRequestOnRelocationTarget() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = state(index, true, ShardRoutingState.RELOCATING);
        final ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metaData().index(index).primaryTerm(shardId.id());
        String primaryTargetNodeId = primaryShard.relocatingNodeId();
        // simulate execution of the primary phase on the relocation target node
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(primaryTargetNodeId)).build();
        setState(clusterService, state);
        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        AtomicBoolean executed = new AtomicBoolean();
        action.new AsyncPrimaryAction(request, primaryShard.allocationId().getRelocationId(), primaryTerm,
            createTransportChannel(listener), task) {
            @Override
            protected ReplicationOperation<Request, Request, TransportReplicationAction.PrimaryResult<Request, TestResponse>>
            createReplicatedOperation(
                    Request request,
                    ActionListener<TransportReplicationAction.PrimaryResult<Request, TestResponse>> actionListener,
                    TransportReplicationAction<Request, Request, TestResponse>.PrimaryShardReference primaryShardReference) {
                return new NoopReplicationOperation(request, actionListener) {
                    public void execute() throws Exception {
                        assertPhase(task, "primary");
                        assertFalse(executed.getAndSet(true));
                        super.execute();
                    }
                };
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException(e);
            }
        }.run();
        assertThat(executed.get(), equalTo(true));
        assertPhase(task, "finished");
        assertFalse(request.isRetrySet.get());
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
        TestAction.PrimaryShardReference primary = action.new PrimaryShardReference(shard, releasable);
        final Request request = new Request();
        Request replicaRequest = (Request) primary.perform(request).replicaRequest;

        final ElasticsearchException exception = new ElasticsearchException("testing");
        primary.failShard("test", exception);

        verify(shard).failShard("test", exception);

        primary.close();

        assertTrue(closed.get());
    }

    public void testReplicaProxy() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = stateWithActivePrimary(index, true, 1 + randomInt(3), randomInt(2));
        logger.info("using state: {}", state);
        setState(clusterService, state);
        ReplicationOperation.Replicas proxy = action.newReplicasProxy(state.metaData().index(index).primaryTerm(0));

        // check that at unknown node fails
        PlainActionFuture<ReplicaResponse> listener = new PlainActionFuture<>();
        ShardRoutingState routingState = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED,
            ShardRoutingState.RELOCATING);
        proxy.performOn(
            TestShardRouting.newShardRouting(shardId, "NOT THERE",
                routingState == ShardRoutingState.RELOCATING ? state.nodes().iterator().next().getId() : null, false, routingState),
                new Request(),
                randomNonNegativeLong(),
                listener);
        assertTrue(listener.isDone());
        assertListenerThrows("non existent node should throw a NoNodeAvailableException", listener, NoNodeAvailableException.class);

        final IndexShardRoutingTable shardRoutings = state.routingTable().shardRoutingTable(shardId);
        final ShardRouting replica = randomFrom(shardRoutings.replicaShards().stream()
            .filter(ShardRouting::assignedToNode).collect(Collectors.toList()));
        listener = new PlainActionFuture<>();
        proxy.performOn(replica, new Request(), randomNonNegativeLong(), listener);
        assertFalse(listener.isDone());

        CapturingTransport.CapturedRequest[] captures = transport.getCapturedRequestsAndClear();
        assertThat(captures, arrayWithSize(1));
        if (randomBoolean()) {
            final TransportReplicationAction.ReplicaResponse response =
                    new TransportReplicationAction.ReplicaResponse(randomLong(), randomLong());
            transport.handleResponse(captures[0].requestId, response);
            assertTrue(listener.isDone());
            assertThat(listener.get(), equalTo(response));
        } else if (randomBoolean()) {
            transport.handleRemoteError(captures[0].requestId, new ElasticsearchException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, ElasticsearchException.class);
        } else {
            transport.handleError(captures[0].requestId, new TransportException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, TransportException.class);
        }

        AtomicReference<Object> failure = new AtomicReference<>();
        AtomicReference<Object> ignoredFailure = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();
        proxy.failShardIfNeeded(replica, "test", new ElasticsearchException("simulated"),
                () -> success.set(true), failure::set, ignoredFailure::set
        );
        CapturingTransport.CapturedRequest[] shardFailedRequests = transport.getCapturedRequestsAndClear();
        // A replication action doesn't not fail the request
        assertEquals(0, shardFailedRequests.length);
    }

    public void testSeqNoIsSetOnPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // we use one replica to check the primary term was set on the operation and sent to the replica
        setState(clusterService,
            state(index, true, ShardRoutingState.STARTED, randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED)));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        final ShardRouting routingEntry = clusterService.state().getRoutingTable().index("test").shard(0).primaryShard();
        final long primaryTerm = clusterService.state().metaData().index(index).primaryTerm(shardId.id());
        Request request = new Request(shardId);
        TransportReplicationAction.ConcreteShardRequest<Request> concreteShardRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, routingEntry.allocationId().getId(), primaryTerm);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();


        final IndexShard shard = mock(IndexShard.class);
        when(shard.getPrimaryTerm()).thenReturn(primaryTerm);
        when(shard.routingEntry()).thenReturn(routingEntry);
        when(shard.getReplicationGroup()).thenReturn(
            new ReplicationGroup(clusterService.state().routingTable().shardRoutingTable(shardId),
                clusterService.state().metaData().index(index).inSyncAllocationIds(0)));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>)invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString());

        AtomicBoolean closed = new AtomicBoolean();
        Releasable releasable = () -> {
            if (closed.compareAndSet(false, true) == false) {
                fail("releasable is closed twice");
            }
        };

        TestAction action =
            new TestAction(Settings.EMPTY, "testSeqNoIsSetOnPrimary", transportService, clusterService, shardStateAction, threadPool) {
                @Override
                protected IndexShard getIndexShard(ShardId shardId) {
                    return shard;
                }
            };

        TransportReplicationAction<Request, Request, TestResponse>.PrimaryOperationTransportHandler primaryPhase =
            action.new PrimaryOperationTransportHandler();
        primaryPhase.messageReceived(concreteShardRequest, createTransportChannel(listener), null);
        CapturingTransport.CapturedRequest[] requestsToReplicas = transport.capturedRequests();
        assertThat(requestsToReplicas, arrayWithSize(1));
        assertThat(((TransportReplicationAction.ConcreteShardRequest<Request>) requestsToReplicas[0].request).getPrimaryTerm(),
            equalTo(primaryTerm));
    }

    public void testCounterOnPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replica, we only want to test on primary
        final ClusterState state = state(index, true, ShardRoutingState.STARTED);
        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        final ShardRouting primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metaData().index(index).primaryTerm(shardId.id());
        Request request = new Request(shardId);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        int i = randomInt(3);
        final boolean throwExceptionOnCreation = i == 1;
        final boolean throwExceptionOnRun = i == 2;
        final boolean respondWithError = i == 3;
        action.new AsyncPrimaryAction(request, primaryShard.allocationId().getId(), primaryTerm, createTransportChannel(listener), task) {
            @Override
            protected ReplicationOperation<Request, Request, TransportReplicationAction.PrimaryResult<Request, TestResponse>>
            createReplicatedOperation(
                    Request request,
                    ActionListener<TransportReplicationAction.PrimaryResult<Request, TestResponse>> actionListener,
                    TransportReplicationAction<Request, Request, TestResponse>.PrimaryShardReference primaryShardReference) {
                assertIndexShardCounter(1);
                if (throwExceptionOnCreation) {
                    throw new ElasticsearchException("simulated exception, during createReplicatedOperation");
                }
                return new NoopReplicationOperation(request, actionListener) {
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
        }.run();
        assertIndexShardCounter(0);
        assertTrue(listener.isDone());
        assertPhase(task, "finished");

        try {
            listener.get();
        } catch (ExecutionException e) {
            if (throwExceptionOnCreation || throwExceptionOnRun || respondWithError) {
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
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        boolean throwException = randomBoolean();
        final ReplicationTask task = maybeTask();
        TestAction action = new TestAction(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, shardStateAction,
            threadPool) {
            @Override
            protected ReplicaResult shardOperationOnReplica(Request request, IndexShard replica) {
                assertIndexShardCounter(1);
                assertPhase(task, "replica");
                if (throwException) {
                    throw new ElasticsearchException("simulated");
                }
                return new ReplicaResult();
            }
        };
        final TestAction.ReplicaOperationTransportHandler replicaOperationTransportHandler = action.new ReplicaOperationTransportHandler();
        try {
            replicaOperationTransportHandler.messageReceived(
                new TransportReplicationAction.ConcreteReplicaRequest<>(
                        new Request().setShardId(shardId), replicaRouting.allocationId().getId(), randomNonNegativeLong(),
                        randomNonNegativeLong()),
                createTransportChannel(new PlainActionFuture<>()), task);
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("simulated"));
            assertTrue(throwException);
        }
        assertPhase(task, "finished");
        // operation should have finished and counter decreased because no outstanding replica requests
        assertIndexShardCounter(0);
    }

    /**
     * This test ensures that replication operations adhere to the {@link IndexMetaData#SETTING_WAIT_FOR_ACTIVE_SHARDS} setting
     * when the request is using the default value for waitForActiveShards.
     */
    public void testDefaultWaitForActiveShardsUsesIndexSetting() throws Exception {
        final String indexName = "test";
        final ShardId shardId = new ShardId(indexName, "_na_", 0);

        // test wait_for_active_shards index setting used when the default is set on the request
        int numReplicas = randomIntBetween(0, 5);
        int idxSettingWaitForActiveShards = randomIntBetween(0, numReplicas + 1);
        ClusterState state = stateWithActivePrimary(indexName, randomBoolean(), numReplicas);
        IndexMetaData indexMetaData = state.metaData().index(indexName);
        Settings indexSettings = Settings.builder().put(indexMetaData.getSettings())
                                     .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(idxSettingWaitForActiveShards))
                                     .build();
        MetaData.Builder metaDataBuilder = MetaData.builder(state.metaData())
                                               .put(IndexMetaData.builder(indexMetaData).settings(indexSettings).build(), true);
        state = ClusterState.builder(state).metaData(metaDataBuilder).build();
        setState(clusterService, state);
        Request request = new Request(shardId).waitForActiveShards(ActiveShardCount.DEFAULT); // set to default so index settings are used
        action.resolveRequest(state.metaData().index(indexName), request);
        assertEquals(ActiveShardCount.from(idxSettingWaitForActiveShards), request.waitForActiveShards());

        // test wait_for_active_shards when default not set on the request (request value should be honored over index setting)
        int requestWaitForActiveShards = randomIntBetween(0, numReplicas + 1);
        request = new Request(shardId).waitForActiveShards(ActiveShardCount.from(requestWaitForActiveShards));
        action.resolveRequest(state.metaData().index(indexName), request);
        assertEquals(ActiveShardCount.from(requestWaitForActiveShards), request.waitForActiveShards());
    }

    /** test that a primary request is rejected if it arrives at a shard with a wrong allocation id or term */
    public void testPrimaryActionRejectsWrongAidOrWrongTerm() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        setState(clusterService, state(index, true, ShardRoutingState.STARTED));
        final ShardRouting primary = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = clusterService.state().metaData().index(shardId.getIndexName()).primaryTerm(shardId.id());
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        final boolean wrongAllocationId = randomBoolean();
        final long requestTerm = wrongAllocationId && randomBoolean() ? primaryTerm : primaryTerm + randomIntBetween(1, 10);
        Request request = new Request(shardId).timeout("1ms");
            action.new PrimaryOperationTransportHandler().messageReceived(
                new TransportReplicationAction.ConcreteShardRequest<>(request,
                    wrongAllocationId ? "_not_a_valid_aid_" : primary.allocationId().getId(),
                    requestTerm),
                createTransportChannel(listener), maybeTask()
            );
        try {
            listener.get();
            fail("using a wrong aid didn't fail the operation");
        } catch (ExecutionException execException) {
            Throwable throwable = execException.getCause();
            logger.debug("got exception:" , throwable);
            assertTrue(throwable.getClass() + " is not a retry exception", action.retryPrimaryException(throwable));
            if (wrongAllocationId) {
                assertThat(throwable.getMessage(), containsString("expected aID [_not_a_valid_aid_] but found [" +
                    primary.allocationId().getId() + "]"));
            } else {
                assertThat(throwable.getMessage(), containsString("expected aID [" + primary.allocationId().getId() + "] with term [" +
                    requestTerm + "] but found [" + primaryTerm + "]"));
            }
        }
    }

    /** test that a replica request is rejected if it arrives at a shard with a wrong allocation id */
    public void testReplicaActionRejectsWrongAid() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = state(index, false, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        final ShardRouting replica = state.routingTable().shardRoutingTable(shardId).replicaShards().get(0);
        // simulate execution of the node holding the replica
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(replica.currentNodeId())).build();
        setState(clusterService, state);

        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        Request request = new Request(shardId).timeout("1ms");
        action.new ReplicaOperationTransportHandler().messageReceived(
            new TransportReplicationAction.ConcreteReplicaRequest<>(request, "_not_a_valid_aid_", randomNonNegativeLong(),
                randomNonNegativeLong()),
            createTransportChannel(listener), maybeTask()
        );
        try {
            listener.get();
            fail("using a wrong aid didn't fail the operation");
        } catch (ExecutionException execException) {
            Throwable throwable = execException.getCause();
            if (action.retryPrimaryException(throwable) == false) {
                throw new AssertionError("thrown exception is not retriable", throwable);
            }
            assertThat(throwable.getMessage(), containsString("_not_a_valid_aid_"));
        }
    }

    /**
     * test throwing a {@link org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException}
     * causes a retry
     */
    public void testRetryOnReplica() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        final ShardRouting replica = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final long primaryTerm = state.metaData().index(shardId.getIndexName()).primaryTerm(shardId.id());
        // simulate execution of the node holding the replica
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(replica.currentNodeId())).build();
        setState(clusterService, state);
        AtomicBoolean throwException = new AtomicBoolean(true);
        final ReplicationTask task = maybeTask();
        TestAction action = new TestAction(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, shardStateAction,
            threadPool) {
            @Override
            protected ReplicaResult shardOperationOnReplica(Request request, IndexShard replica) {
                assertPhase(task, "replica");
                if (throwException.get()) {
                    throw new RetryOnReplicaException(shardId, "simulation");
                }
                return new ReplicaResult();
            }
        };
        final TestAction.ReplicaOperationTransportHandler replicaOperationTransportHandler = action.new ReplicaOperationTransportHandler();
        final PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        final Request request = new Request().setShardId(shardId);
        final long checkpoint = randomNonNegativeLong();
        replicaOperationTransportHandler.messageReceived(
                new TransportReplicationAction.ConcreteReplicaRequest<>(request, replica.allocationId().getId(), primaryTerm, checkpoint),
                createTransportChannel(listener), task);
        if (listener.isDone()) {
            listener.get(); // fail with the exception if there
            fail("listener shouldn't be done");
        }

        // no retry yet
        List<CapturingTransport.CapturedRequest> capturedRequests =
            transport.getCapturedRequestsByTargetNodeAndClear().get(replica.currentNodeId());
        assertThat(capturedRequests, nullValue());

        // release the waiting
        throwException.set(false);
        setState(clusterService, state);

        capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear().get(replica.currentNodeId());
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        final CapturingTransport.CapturedRequest capturedRequest = capturedRequests.get(0);
        assertThat(capturedRequest.action, equalTo("testActionWithExceptions[r]"));
        assertThat(capturedRequest.request, instanceOf(TransportReplicationAction.ConcreteReplicaRequest.class));
        assertThat(((TransportReplicationAction.ConcreteReplicaRequest) capturedRequest.request).getGlobalCheckpoint(),
                equalTo(checkpoint));
        assertConcreteShardRequest(capturedRequest.request, request, replica.allocationId());
    }

    public void testRetryOnReplicaWithRealTransport() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState initialState = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        final ShardRouting replica = initialState.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final long primaryTerm = initialState.metaData().index(shardId.getIndexName()).primaryTerm(shardId.id());
        // simulate execution of the node holding the replica
        final ClusterState stateWithNodes = ClusterState.builder(initialState)
                .nodes(DiscoveryNodes.builder(initialState.nodes()).localNodeId(replica.currentNodeId())).build();
        setState(clusterService, stateWithNodes);
        AtomicBoolean throwException = new AtomicBoolean(true);
        final ReplicationTask task = maybeTask();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final Transport transport = new MockTcpTransport(Settings.EMPTY, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
                new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()),
                Version.CURRENT);
        transportService = new MockTransportService(Settings.EMPTY, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> clusterService.localNode(),null);
        transportService.start();
        transportService.acceptIncomingRequests();

        AtomicBoolean calledSuccessfully = new AtomicBoolean(false);
        TestAction action = new TestAction(Settings.EMPTY, "testActionWithExceptions", transportService, clusterService, shardStateAction,
            threadPool) {
            @Override
            protected ReplicaResult shardOperationOnReplica(Request request, IndexShard replica) {
                assertPhase(task, "replica");
                if (throwException.get()) {
                    throw new RetryOnReplicaException(shardId, "simulation");
                }
                calledSuccessfully.set(true);
                return new ReplicaResult();
            }
        };
        final TestAction.ReplicaOperationTransportHandler replicaOperationTransportHandler = action.new ReplicaOperationTransportHandler();
        final PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        final Request request = new Request().setShardId(shardId);
        final long checkpoint = randomNonNegativeLong();
        replicaOperationTransportHandler.messageReceived(
                new TransportReplicationAction.ConcreteReplicaRequest<>(request, replica.allocationId().getId(), primaryTerm, checkpoint),
                createTransportChannel(listener), task);
        if (listener.isDone()) {
            listener.get(); // fail with the exception if there
            fail("listener shouldn't be done");
        }

        // release the waiting
        throwException.set(false);
        // publish a new state (same as the old state with the version incremented)
        setState(clusterService, stateWithNodes);

        // Assert that the request was retried, this time successfull
        assertTrue("action should have been successfully called on retry but was not", calledSuccessfully.get());
        transportService.stop();
    }

    private void assertConcreteShardRequest(TransportRequest capturedRequest, Request expectedRequest, AllocationId expectedAllocationId) {
        final TransportReplicationAction.ConcreteShardRequest<?> concreteShardRequest =
            (TransportReplicationAction.ConcreteShardRequest<?>) capturedRequest;
        assertThat(concreteShardRequest.getRequest(), equalTo(expectedRequest));
        assertThat(((Request)concreteShardRequest.getRequest()).isRetrySet.get(), equalTo(true));
        assertThat(concreteShardRequest.getTargetAllocationID(), equalTo(expectedAllocationId.getId()));
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
        public AtomicBoolean isRetrySet = new AtomicBoolean(false);

        public Request() {
        }

        Request(ShardId shardId) {
            this();
            this.shardId = shardId;
            this.index = shardId.getIndexName();
            this.waitForActiveShards = ActiveShardCount.NONE;
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

        @Override
        public void onRetry() {
            super.onRetry();
            isRetrySet.set(true);
        }

        @Override
        public String toString() {
            return "Request{}";
        }
    }

    static class TestResponse extends ReplicationResponse {
    }

    private class TestAction extends TransportReplicationAction<Request, Request, TestResponse> {

        private final boolean withDocumentFailureOnPrimary;
        private final boolean withDocumentFailureOnReplica;

        TestAction(Settings settings, String actionName, TransportService transportService,
                   ClusterService clusterService, ShardStateAction shardStateAction,
                   ThreadPool threadPool) {
            super(settings, actionName, transportService, clusterService, mockIndicesService(clusterService), threadPool,
                shardStateAction,
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                Request::new, Request::new, ThreadPool.Names.SAME);
            this.withDocumentFailureOnPrimary = false;
            this.withDocumentFailureOnReplica = false;
        }

        TestAction(Settings settings, String actionName, TransportService transportService,
                   ClusterService clusterService, ShardStateAction shardStateAction,
                   ThreadPool threadPool, boolean withDocumentFailureOnPrimary, boolean withDocumentFailureOnReplica) {
            super(settings, actionName, transportService, clusterService, mockIndicesService(clusterService), threadPool,
                shardStateAction,
                new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                Request::new, Request::new, ThreadPool.Names.SAME);
            this.withDocumentFailureOnPrimary = withDocumentFailureOnPrimary;
            this.withDocumentFailureOnReplica = withDocumentFailureOnReplica;
        }

        @Override
        protected TestResponse newResponseInstance() {
            return new TestResponse();
        }

        @Override
        protected PrimaryResult shardOperationOnPrimary(Request shardRequest, IndexShard primary) throws Exception {
            boolean executedBefore = shardRequest.processedOnPrimary.getAndSet(true);
            assert executedBefore == false : "request has already been executed on the primary";
            return new PrimaryResult(shardRequest, new TestResponse());
        }

        @Override
        protected ReplicaResult shardOperationOnReplica(Request request, IndexShard replica) {
            request.processedOnReplicas.incrementAndGet();
            return new ReplicaResult();
        }

        @Override
        protected boolean resolveIndex() {
            return false;
        }
    }

    final IndicesService mockIndicesService(ClusterService clusterService) {
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).then(invocation -> {
            Index index = (Index)invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            final IndexMetaData indexSafe = state.metaData().getIndexSafe(index);
            return mockIndexService(indexSafe, clusterService);
        });
        when(indicesService.indexService(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            if (state.metaData().hasIndex(index.getName())) {
                final IndexMetaData indexSafe = state.metaData().getIndexSafe(index);
                return mockIndexService(clusterService.state().metaData().getIndexSafe(index), clusterService);
            } else {
                return null;
            }
        });
        return indicesService;
    }

    final IndexService mockIndexService(final IndexMetaData indexMetaData, ClusterService clusterService) {
        final IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).then(invocation -> {
            int shard = (Integer) invocation.getArguments()[0];
            final ShardId shardId = new ShardId(indexMetaData.getIndex(), shard);
            if (shard > indexMetaData.getNumberOfShards()) {
                throw new ShardNotFoundException(shardId);
            }
            return mockIndexShard(shardId, clusterService);
        });
        return indexService;
    }

    private IndexShard mockIndexShard(ShardId shardId, ClusterService clusterService) {
        final IndexShard indexShard = mock(IndexShard.class);
        doAnswer(invocation -> {
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquirePrimaryOperationPermit(any(ActionListener.class), anyString());
        doAnswer(invocation -> {
            long term = (Long)invocation.getArguments()[0];
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[2];
            final long primaryTerm = indexShard.getPrimaryTerm();
            if (term < primaryTerm) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "%s operation term [%d] is too old (current [%d])",
                    shardId, term, primaryTerm));
            }
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquireReplicaOperationPermit(anyLong(), anyLong(), any(ActionListener.class), anyString());
        when(indexShard.routingEntry()).thenAnswer(invocationOnMock -> {
            final ClusterState state = clusterService.state();
            final RoutingNode node = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
            final ShardRouting routing = node.getByShardId(shardId);
            if (routing == null) {
                throw new ShardNotFoundException(shardId, "shard is no longer assigned to current node");
            }
            return routing;
        });
        when(indexShard.state()).thenAnswer(invocationOnMock -> isRelocated.get() ? IndexShardState.RELOCATED : IndexShardState.STARTED);
        doThrow(new AssertionError("failed shard is not supported")).when(indexShard).failShard(anyString(), any(Exception.class));
        when(indexShard.getPrimaryTerm()).thenAnswer(i ->
            clusterService.state().metaData().getIndexSafe(shardId.getIndex()).primaryTerm(shardId.id()));
        return indexShard;
    }

    class NoopReplicationOperation extends ReplicationOperation<Request, Request, TestAction.PrimaryResult<Request, TestResponse>> {

        NoopReplicationOperation(Request request, ActionListener<TestAction.PrimaryResult<Request, TestResponse>> listener) {
            super(request, null, listener, null, TransportReplicationActionTests.this.logger, "noop");
        }

        @Override
        public void execute() throws Exception {
            // Using the diamond operator (<>) prevents Eclipse from being able to compile this code
            this.resultListener.onResponse(new TransportReplicationAction.PrimaryResult<Request, TestResponse>(null, new TestResponse()));
        }
    }

    /**
     * Transport channel that is needed for replica operation testing.
     */
    public TransportChannel createTransportChannel(final PlainActionFuture<TestResponse> listener) {
        return new TransportChannel() {

            @Override
            public String getProfileName() {
                return "";
            }

            @Override
            public void sendResponse(TransportResponse response) throws IOException {
                listener.onResponse(((TestResponse) response));
            }

            @Override
            public void sendResponse(TransportResponse response, TransportResponseOptions options) throws IOException {
                listener.onResponse(((TestResponse) response));
            }

            @Override
            public void sendResponse(Exception exception) throws IOException {
                listener.onFailure(exception);
            }

            @Override
            public String getChannelType() {
                return "replica_test";
            }
        };
    }

}
