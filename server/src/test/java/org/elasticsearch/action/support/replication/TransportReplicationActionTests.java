/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.shard.ShardNotInPrimaryModeException;
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
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.stateWithActivePrimary;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportReplicationActionTests extends ESTestCase {

    private static final ShardId NO_SHARD_ID = null;

    /**
     * takes a request that was sent by a {@link TransportReplicationAction} and captured
     * and returns the underlying request if it's wrapped or the original (cast to the expected type).
     *
     * This will throw a {@link ClassCastException} if the request is of the wrong type.
     */
    @SuppressWarnings("unchecked")
    public static <R extends ReplicationRequest<?>> R resolveRequest(TransportRequest requestOrWrappedRequest) {
        if (requestOrWrappedRequest instanceof TransportReplicationAction.ConcreteShardRequest) {
            requestOrWrappedRequest = ((TransportReplicationAction.ConcreteShardRequest<?>) requestOrWrappedRequest).getRequest();
        }
        return (R) requestOrWrappedRequest;
    }

    private static ThreadPool threadPool;

    private boolean forceExecute;
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
        forceExecute = randomBoolean();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        action = new TestAction(Settings.EMPTY, "internal:testAction", transportService, clusterService, shardStateAction, threadPool);
    }

    @Override
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

    private <T> T assertListenerThrows(String msg, PlainActionFuture<?> listener, Class<T> klass) {
        ExecutionException exception = expectThrows(ExecutionException.class, msg, listener::get);
        assertThat(exception.getCause(), instanceOf(klass));
        @SuppressWarnings("unchecked")
        final T cause = (T) exception.getCause();
        return cause;
    }

    private void setStateWithBlock(final ClusterService clusterService, final ClusterBlock block, final boolean globalBlock) {
        final ClusterBlocks.Builder blocks = ClusterBlocks.builder();
        if (globalBlock) {
            blocks.addGlobalBlock(block);
        } else {
            blocks.addIndexBlock("index", block);
        }
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(blocks).build());
    }

    public void testBlocksInReroutePhase() {
        final ClusterBlock nonRetryableBlock = new ClusterBlock(
            1,
            "non retryable",
            false,
            true,
            false,
            RestStatus.SERVICE_UNAVAILABLE,
            ClusterBlockLevel.ALL
        );
        final ClusterBlock retryableBlock = new ClusterBlock(
            1,
            "retryable",
            true,
            true,
            false,
            RestStatus.SERVICE_UNAVAILABLE,
            ClusterBlockLevel.ALL
        );

        final boolean globalBlock = randomBoolean();
        final TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testActionWithBlocks",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        ) {
            @Override
            protected ClusterBlockLevel globalBlockLevel() {
                return globalBlock ? ClusterBlockLevel.WRITE : null;
            }

            @Override
            public ClusterBlockLevel indexBlockLevel() {
                return globalBlock == false ? ClusterBlockLevel.WRITE : null;
            }
        };

        setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary("index", true, 0));

        ShardId shardId = new ShardId(clusterService.state().metadata().index("index").getIndex(), 0);

        {
            setStateWithBlock(clusterService, nonRetryableBlock, globalBlock);

            Request request = new Request(shardId);
            PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
            ReplicationTask task = maybeTask();

            TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
            reroutePhase.run();

            ClusterBlockException exception = assertListenerThrows(
                "primary action should fail operation",
                listener,
                ClusterBlockException.class
            );
            assertThat(((ClusterBlockException) exception.unwrapCause()).blocks().iterator().next(), is(nonRetryableBlock));
            assertPhase(task, "failed");
        }
        {
            setStateWithBlock(clusterService, retryableBlock, globalBlock);

            Request requestWithTimeout = (globalBlock ? new Request(shardId) : new Request(shardId)).timeout("5ms");
            PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
            ReplicationTask task = maybeTask();

            TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, requestWithTimeout, listener);
            reroutePhase.run();

            ClusterBlockException exception = assertListenerThrows(
                "failed to timeout on retryable block",
                listener,
                ClusterBlockException.class
            );
            assertThat(((ClusterBlockException) exception.unwrapCause()).blocks().iterator().next(), is(retryableBlock));
            assertPhase(task, "failed");
            assertTrue(requestWithTimeout.isRetrySet.get());
        }
        {
            setStateWithBlock(clusterService, retryableBlock, globalBlock);

            Request request = new Request(shardId);
            PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
            ReplicationTask task = maybeTask();

            TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
            reroutePhase.run();

            assertFalse("primary phase should wait on retryable block", listener.isDone());
            assertPhase(task, "waiting_for_retry");
            assertTrue(request.isRetrySet.get());

            setStateWithBlock(clusterService, nonRetryableBlock, globalBlock);

            ClusterBlockException exception = assertListenerThrows(
                "primary phase should fail operation when moving from a retryable " + "block to a non-retryable one",
                listener,
                ClusterBlockException.class
            );
            assertThat(((ClusterBlockException) exception.unwrapCause()).blocks().iterator().next(), is(nonRetryableBlock));
            assertIndexShardUninitialized();
        }
        {
            Request requestWithTimeout = new Request(new ShardId("unknown", "_na_", 0)).index("unknown").timeout("5ms");
            PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
            ReplicationTask task = maybeTask();

            TestAction testActionWithNoBlocks = new TestAction(
                Settings.EMPTY,
                "internal:testActionWithNoBlocks",
                transportService,
                clusterService,
                shardStateAction,
                threadPool
            );
            TestAction.ReroutePhase reroutePhase = testActionWithNoBlocks.new ReroutePhase(task, requestWithTimeout, listener);
            reroutePhase.run();
            assertListenerThrows("should fail with an IndexNotFoundException when no blocks", listener, IndexNotFoundException.class);
        }
    }

    public void testBlocksInPrimaryAction() {
        final boolean globalBlock = randomBoolean();

        final TestAction actionWithBlocks = new TestAction(
            Settings.EMPTY,
            "internal:actionWithBlocks",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        ) {
            @Override
            protected ClusterBlockLevel globalBlockLevel() {
                return globalBlock ? ClusterBlockLevel.WRITE : null;
            }

            @Override
            public ClusterBlockLevel indexBlockLevel() {
                return globalBlock == false ? ClusterBlockLevel.WRITE : null;
            }
        };

        final String index = "index";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        setState(clusterService, stateWithActivePrimary(index, true, randomInt(5)));

        final ClusterBlocks.Builder block = ClusterBlocks.builder();
        if (globalBlock) {
            block.addGlobalBlock(
                new ClusterBlock(
                    randomIntBetween(1, 16),
                    "test global block",
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    RestStatus.BAD_REQUEST,
                    ClusterBlockLevel.ALL
                )
            );
        } else {
            block.addIndexBlock(
                index,
                new ClusterBlock(
                    randomIntBetween(1, 16),
                    "test index block",
                    randomBoolean(),
                    randomBoolean(),
                    randomBoolean(),
                    RestStatus.FORBIDDEN,
                    ClusterBlockLevel.READ_WRITE
                )
            );
        }
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));

        final ClusterState clusterState = clusterService.state();
        final String targetAllocationID = clusterState.getRoutingTable().shardRoutingTable(shardId).primaryShard().allocationId().getId();
        final long primaryTerm = clusterState.metadata().index(index).primaryTerm(shardId.id());
        final Request request = new Request(shardId);
        final ReplicationTask task = maybeTask();
        final PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();

        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, targetAllocationID, primaryTerm);
        @SuppressWarnings("rawtypes")
        final TransportReplicationAction.AsyncPrimaryAction asyncPrimaryActionWithBlocks = actionWithBlocks.new AsyncPrimaryAction(
            primaryRequest, listener, task
        );
        asyncPrimaryActionWithBlocks.run();

        final ExecutionException exception = expectThrows(ExecutionException.class, listener::get);
        assertThat(exception.getCause(), instanceOf(ClusterBlockException.class));
        assertThat(exception.getCause(), hasToString(containsString("test " + (globalBlock ? "global" : "index") + " block")));
        assertPhase(task, "finished");
    }

    public void assertIndexShardUninitialized() {
        assertEquals(0, count.get());
    }

    public void testNotStartedPrimary() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replicas in oder to skip the replication part
        setState(clusterService, state(index, true, randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));
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
        final List<CapturingTransport.CapturedRequest> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear()
            .get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        assertThat(capturedRequests.get(0).action(), equalTo("internal:testAction[p]"));
        assertIndexShardCounter(0);
    }

    public void testShardNotInPrimaryMode() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        final ClusterState state = state(index, true, ShardRoutingState.RELOCATING);
        setState(clusterService, state);
        final ReplicationTask task = maybeTask();
        final Request request = new Request(shardId);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        final AtomicBoolean executed = new AtomicBoolean();

        final ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metadata().index(index).primaryTerm(shardId.id());
        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, primaryShard.allocationId().getId(), primaryTerm);

        isPrimaryMode.set(false);

        new TestAction(Settings.EMPTY, "internal:test-action", transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected void shardOperationOnPrimary(
                Request shardRequest,
                IndexShard primary,
                ActionListener<PrimaryResult<Request, TestResponse>> listener
            ) {
                assertPhase(task, "primary");
                assertFalse(executed.getAndSet(true));
                super.shardOperationOnPrimary(shardRequest, primary, listener);
            }
        }.new AsyncPrimaryAction(primaryRequest, listener, task).run();

        assertFalse(executed.get());
        assertIndexShardCounter(0);  // no permit should be held

        final ExecutionException e = expectThrows(ExecutionException.class, listener::get);
        assertThat(e.getCause(), instanceOf(ReplicationOperation.RetryOnPrimaryException.class));
        assertThat(e.getCause(), hasToString(containsString("shard is not in primary mode")));
        assertThat(e.getCause().getCause(), instanceOf(ShardNotInPrimaryModeException.class));
        assertThat(e.getCause().getCause(), hasToString(containsString("shard is not in primary mode")));
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
    public void testNoRerouteOnStaleClusterState() {
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
        ShardRouting relocationTarget = clusterService.state()
            .getRoutingTable()
            .shardRoutingTable(shardId)
            .shardsWithState(ShardRoutingState.INITIALIZING)
            .get(0);
        AllocationService allocationService = ESAllocationTestCase.createAllocationService();
        ClusterState updatedState = ESAllocationTestCase.startShardsAndReroute(allocationService, state, relocationTarget);

        setState(clusterService, updatedState);
        logger.debug("--> relocation complete state:\n{}", clusterService.state());

        IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().index(index).shard(shardId.id());
        final String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
        final List<CapturingTransport.CapturedRequest> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear()
            .get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        assertThat(capturedRequests.get(0).action(), equalTo("internal:testAction[p]"));
        assertIndexShardCounter(0);
    }

    public void testUnknownIndexOrShardOnReroute() {
        final String index = "test";
        // no replicas in oder to skip the replication part
        setState(clusterService, state(index, true, randomBoolean() ? ShardRoutingState.INITIALIZING : ShardRoutingState.UNASSIGNED));
        logger.debug("--> using initial state:\n{}", clusterService.state());

        Request request = new Request(new ShardId("unknown_index", "_na_", 0)).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw index not found exception", listener, IndexNotFoundException.class);
        assertPhase(task, "failed");
        assertFalse(request.isRetrySet.get());

        // try again with a request that is based on a newer cluster state, make sure we waited until that
        // cluster state for the index to appear
        request = new Request(new ShardId("unknown_index", "_na_", 0)).timeout("1ms");
        request.routedBasedOnClusterVersion(clusterService.state().version() + 1);
        listener = new PlainActionFuture<>();
        task = maybeTask();

        reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw index not found exception", listener, IndexNotFoundException.class);
        assertPhase(task, "failed");
        assertTrue(request.isRetrySet.get());

        request = new Request(new ShardId(index, "_na_", 10)).timeout("1ms");
        listener = new PlainActionFuture<>();
        reroutePhase = action.new ReroutePhase(null, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw shard not found exception", listener, ShardNotFoundException.class);
        assertFalse(request.isRetrySet.get()); // TODO I'd have expected this to be true but we fail too early?
    }

    public void testClosedIndexOnReroute() {
        final String index = "test";
        // no replicas in oder to skip the replication part
        ClusterStateChanges clusterStateChanges = new ClusterStateChanges(xContentRegistry(), threadPool);
        setState(
            clusterService,
            clusterStateChanges.closeIndices(
                clusterStateChanges.createIndex(
                    clusterService.state(),
                    new CreateIndexRequest(index).waitForActiveShards(ActiveShardCount.NONE)
                ),
                new CloseIndexRequest(index)
            )
        );
        assertThat(clusterService.state().metadata().indices().get(index).getState(), equalTo(IndexMetadata.State.CLOSE));
        logger.debug("--> using initial state:\n{}", clusterService.state());
        Request request = new Request(new ShardId(clusterService.state().metadata().indices().get(index).getIndex(), 0)).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();

        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testActionWithBlocks",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );
        TestAction.ReroutePhase reroutePhase = action.new ReroutePhase(task, request, listener);
        reroutePhase.run();
        assertListenerThrows("must throw index closed exception", listener, IndexClosedException.class);

        assertPhase(task, "failed");
        assertFalse(request.isRetrySet.get());
    }

    public void testStalePrimaryShardOnReroute() {
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
        assertThat(capturedRequests[0].action(), equalTo("internal:testAction[p]"));
        assertPhase(task, "waiting_on_primary");
        assertFalse(request.isRetrySet.get());
        transport.handleRemoteError(capturedRequests[0].requestId(), randomRetryPrimaryException(shardId));

        if (timeout) {
            // we always try at least one more time on timeout
            assertThat(listener.isDone(), equalTo(false));
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests, arrayWithSize(1));
            assertThat(capturedRequests[0].action(), equalTo("internal:testAction[p]"));
            assertPhase(task, "waiting_on_primary");
            transport.handleRemoteError(capturedRequests[0].requestId(), randomRetryPrimaryException(shardId));
            assertListenerThrows("must throw index not found exception", listener, ElasticsearchException.class);
            assertPhase(task, "failed");
        } else {
            assertThat(listener.isDone(), equalTo(false));
            // generate a CS change
            setState(clusterService, clusterService.state());
            capturedRequests = transport.getCapturedRequestsAndClear();
            assertThat(capturedRequests, arrayWithSize(1));
            assertThat(capturedRequests[0].action(), equalTo("internal:testAction[p]"));
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
        final List<CapturingTransport.CapturedRequest> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear()
            .get(primaryNodeId);
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        if (clusterService.state().nodes().getLocalNodeId().equals(primaryNodeId)) {
            assertThat(capturedRequests.get(0).action(), equalTo("internal:testAction[p]"));
            assertPhase(task, "waiting_on_primary");
        } else {
            assertThat(capturedRequests.get(0).action(), equalTo("internal:testAction"));
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
        final long primaryTerm = state.metadata().index(index).primaryTerm(shardId.id());
        boolean executeOnPrimary = true;
        // whether shard has been marked as relocated already (i.e. relocation completed)
        if (primaryShard.relocating() && randomBoolean()) {
            isRelocated.set(true);
            executeOnPrimary = false;
        }
        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, primaryShard.allocationId().getId(), primaryTerm);

        new TestAction(Settings.EMPTY, "internal:testAction2", transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected void shardOperationOnPrimary(
                Request shardRequest,
                IndexShard primary,
                ActionListener<PrimaryResult<Request, TestResponse>> listener
            ) {
                assertPhase(task, "primary");
                assertFalse(executed.getAndSet(true));
                super.shardOperationOnPrimary(shardRequest, primary, listener);
            }
        }.new AsyncPrimaryAction(primaryRequest, listener, task).run();

        if (executeOnPrimary) {
            assertTrue(executed.get());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
            assertFalse(request.isRetrySet.get());
        } else {
            assertFalse(executed.get());
            assertIndexShardCounter(0);  // it should have been freed.
            final List<CapturingTransport.CapturedRequest> requests = transport.capturedRequestsByTargetNode()
                .get(primaryShard.relocatingNodeId());
            assertThat(requests, notNullValue());
            assertThat(requests.size(), equalTo(1));
            assertThat(
                "primary request was not delegated to relocation target",
                requests.get(0).action(),
                equalTo("internal:testAction2[p]")
            );
            @SuppressWarnings("unchecked")
            final TransportReplicationAction.ConcreteShardRequest<Request> concreteShardRequest =
                (TransportReplicationAction.ConcreteShardRequest<Request>) requests.get(0).request();
            assertThat("primary term not properly set on primary delegation", concreteShardRequest.getPrimaryTerm(), equalTo(primaryTerm));
            assertPhase(task, "primary_delegation");
            transport.handleResponse(requests.get(0).requestId(), new TestResponse());
            assertTrue(listener.isDone());
            listener.get();
            assertPhase(task, "finished");
            assertFalse(request.isRetrySet.get());
        }
    }

    public void testPrimaryPhaseExecutesDelegatedRequestOnRelocationTarget() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = state(index, true, ShardRoutingState.RELOCATING);
        final ShardRouting primaryShard = state.getRoutingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metadata().index(index).primaryTerm(shardId.id());
        String primaryTargetNodeId = primaryShard.relocatingNodeId();
        // simulate execution of the primary phase on the relocation target node
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(primaryTargetNodeId)).build();
        setState(clusterService, state);
        Request request = new Request(shardId).timeout("1ms");
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        AtomicBoolean executed = new AtomicBoolean();
        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, primaryShard.allocationId().getRelocationId(), primaryTerm);

        new TestAction(Settings.EMPTY, "internal:testAction2", transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected void shardOperationOnPrimary(
                Request shardRequest,
                IndexShard primary,
                ActionListener<PrimaryResult<Request, TestResponse>> listener
            ) {
                assertPhase(task, "primary");
                assertFalse(executed.getAndSet(true));
                super.shardOperationOnPrimary(shardRequest, primary, listener);
            }
        }.new AsyncPrimaryAction(primaryRequest, listener, task).run();
        assertThat(executed.get(), equalTo(true));
        assertPhase(task, "finished");
        assertFalse(request.isRetrySet.get());
        assertTrue(listener.isDone());
        listener.actionGet(); // throws no exception
    }

    public void testPrimaryReference() {
        final IndexShard shard = mock(IndexShard.class);

        AtomicBoolean closed = new AtomicBoolean();
        Releasable releasable = () -> {
            if (closed.compareAndSet(false, true) == false) {
                fail("releasable is closed twice");
            }
        };
        TestAction.PrimaryShardReference primary = action.new PrimaryShardReference(shard, releasable);
        final Request request = new Request(NO_SHARD_ID);
        shard.runUnderPrimaryPermit(() -> primary.perform(request, ActionTestUtils.assertNoFailureListener(r -> {
            final ElasticsearchException exception = new ElasticsearchException("testing");
            primary.failShard("test", exception);

            verify(shard).failShard("test", exception);

            primary.close();

            assertTrue(closed.get());
        })), Assert::assertNotNull, null);
    }

    public void testReplicaProxy() throws InterruptedException, ExecutionException {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = stateWithActivePrimary(index, true, 1 + randomInt(3), randomInt(2));
        logger.info("using state: {}", state);
        setState(clusterService, state);
        final long primaryTerm = state.metadata().index(index).primaryTerm(0);
        ReplicationOperation.Replicas<Request> proxy = action.newReplicasProxy();

        // check that at unknown node fails
        PlainActionFuture<ReplicaResponse> listener = new PlainActionFuture<>();
        ShardRoutingState routingState = randomFrom(
            ShardRoutingState.INITIALIZING,
            ShardRoutingState.STARTED,
            ShardRoutingState.RELOCATING
        );
        proxy.performOn(
            TestShardRouting.newShardRouting(
                shardId,
                "NOT THERE",
                routingState == ShardRoutingState.RELOCATING ? state.nodes().iterator().next().getId() : null,
                false,
                routingState
            ),
            new Request(NO_SHARD_ID),
            primaryTerm,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            listener
        );
        assertTrue(listener.isDone());
        assertListenerThrows("non existent node should throw a NoNodeAvailableException", listener, NoNodeAvailableException.class);

        final IndexShardRoutingTable shardRoutings = state.routingTable().shardRoutingTable(shardId);
        final ShardRouting replica = randomFrom(shardRoutings.replicaShards().stream().filter(ShardRouting::assignedToNode).toList());
        listener = new PlainActionFuture<>();
        proxy.performOn(replica, new Request(NO_SHARD_ID), primaryTerm, randomNonNegativeLong(), randomNonNegativeLong(), listener);
        assertFalse(listener.isDone());

        CapturingTransport.CapturedRequest[] captures = transport.getCapturedRequestsAndClear();
        assertThat(captures, arrayWithSize(1));
        if (randomBoolean()) {
            final TransportReplicationAction.ReplicaResponse response = new TransportReplicationAction.ReplicaResponse(
                randomLong(),
                randomLong()
            );
            transport.handleResponse(captures[0].requestId(), response);
            assertTrue(listener.isDone());
            assertThat(listener.get(), equalTo(response));
        } else if (randomBoolean()) {
            transport.handleRemoteError(captures[0].requestId(), new ElasticsearchException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, ElasticsearchException.class);
        } else {
            transport.handleError(captures[0].requestId(), new TransportException("simulated"));
            assertTrue(listener.isDone());
            assertListenerThrows("listener should reflect remote error", listener, TransportException.class);
        }

        AtomicReference<Object> failure = new AtomicReference<>();
        AtomicBoolean success = new AtomicBoolean();
        proxy.failShardIfNeeded(
            replica,
            primaryTerm,
            "test",
            new ElasticsearchException("simulated"),
            ActionListener.wrap(r -> success.set(true), failure::set)
        );
        CapturingTransport.CapturedRequest[] shardFailedRequests = transport.getCapturedRequestsAndClear();
        // A replication action doesn't not fail the request
        assertEquals(0, shardFailedRequests.length);
    }

    public void testSeqNoIsSetOnPrimary() {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // we use one replica to check the primary term was set on the operation and sent to the replica
        setState(
            clusterService,
            state(index, true, ShardRoutingState.STARTED, randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED))
        );
        logger.debug("--> using initial state:\n{}", clusterService.state());
        final ShardRouting routingEntry = clusterService.state().getRoutingTable().index("test").shard(0).primaryShard();
        final long primaryTerm = clusterService.state().metadata().index(index).primaryTerm(shardId.id());
        Request request = new Request(shardId);
        TransportReplicationAction.ConcreteShardRequest<Request> concreteShardRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, routingEntry.allocationId().getId(), primaryTerm);
        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();

        final IndexShard shard = mockIndexShard(shardId, clusterService);
        when(shard.getPendingPrimaryTerm()).thenReturn(primaryTerm);
        when(shard.routingEntry()).thenReturn(routingEntry);
        when(shard.isRelocatedPrimary()).thenReturn(false);
        IndexShardRoutingTable shardRoutingTable = clusterService.state().routingTable().shardRoutingTable(shardId);
        Set<String> inSyncIds = randomBoolean()
            ? singleton(routingEntry.allocationId().getId())
            : clusterService.state().metadata().index(index).inSyncAllocationIds(0);
        ReplicationGroup replicationGroup = new ReplicationGroup(
            shardRoutingTable,
            inSyncIds,
            shardRoutingTable.getPromotableAllocationIds(),
            0
        );
        when(shard.getReplicationGroup()).thenReturn(replicationGroup);
        PendingReplicationActions replicationActions = new PendingReplicationActions(shardId, threadPool);
        replicationActions.accept(replicationGroup);
        when(shard.getPendingReplicationActions()).thenReturn(replicationActions);
        doAnswer(invocation -> {
            count.incrementAndGet();
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> argument = (ActionListener<Releasable>) invocation.getArguments()[0];
            argument.onResponse(count::decrementAndGet);
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), eq(forceExecute));
        when(shard.getActiveOperationsCount()).thenAnswer(i -> count.get());

        final IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(shard.shardId().id())).thenReturn(shard);

        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(shard.shardId().getIndex())).thenReturn(indexService);

        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testSeqNoIsSetOnPrimary",
            transportService,
            clusterService,
            shardStateAction,
            threadPool,
            indicesService
        );

        action.handlePrimaryRequest(concreteShardRequest, createTransportChannel(listener), null);
        CapturingTransport.CapturedRequest[] requestsToReplicas = transport.capturedRequests();
        assertThat(requestsToReplicas, arrayWithSize(1));
        @SuppressWarnings("unchecked")
        TransportReplicationAction.ConcreteShardRequest<Request> shardRequest = (TransportReplicationAction.ConcreteShardRequest<
            Request>) requestsToReplicas[0].request();
        assertThat(shardRequest.getPrimaryTerm(), equalTo(primaryTerm));
    }

    public void testCounterOnPrimary() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        // no replica, we only want to test on primary
        final ClusterState state = state(index, true, ShardRoutingState.STARTED);
        setState(clusterService, state);
        logger.debug("--> using initial state:\n{}", clusterService.state());
        final ShardRouting primaryShard = state.routingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = state.metadata().index(index).primaryTerm(shardId.id());
        Request request = new Request(shardId);
        PlainActionFuture<TestResponse> listener = new PlainActionFuture<>();
        ReplicationTask task = maybeTask();
        int i = randomInt(2);
        final boolean throwExceptionOnRun = i == 1;
        final boolean respondWithError = i == 2;
        final TransportReplicationAction.ConcreteShardRequest<Request> primaryRequest =
            new TransportReplicationAction.ConcreteShardRequest<>(request, primaryShard.allocationId().getId(), primaryTerm);

        new TestAction(Settings.EMPTY, "internal:testAction2", transportService, clusterService, shardStateAction, threadPool) {
            @Override
            protected void shardOperationOnPrimary(
                Request shardRequest,
                IndexShard primary,
                ActionListener<PrimaryResult<Request, TestResponse>> listener
            ) {
                assertIndexShardCounter(1);
                if (throwExceptionOnRun) {
                    throw new ElasticsearchException("simulated exception, during shardOperationOnPrimary");
                } else if (respondWithError) {
                    listener.onFailure(new ElasticsearchException("simulated exception, as a response"));
                } else {
                    super.shardOperationOnPrimary(request, primary, listener);
                }
            }
        }.new AsyncPrimaryAction(primaryRequest, listener, task).run();

        assertIndexShardCounter(0);
        assertTrue(listener.isDone());
        assertPhase(task, "finished");

        try {
            listener.get();
            if (throwExceptionOnRun || respondWithError) {
                fail("expected exception, but none was thrown");
            }
        } catch (ExecutionException e) {
            if (throwExceptionOnRun || respondWithError) {
                Throwable cause = e.getCause();
                assertThat(cause, instanceOf(ElasticsearchException.class));
                assertThat(cause.getMessage(), containsString("simulated"));
            } else {
                throw e;
            }
        }
    }

    public void testReplicasCounter() {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState state = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        setState(clusterService, state);
        final ShardRouting replicaRouting = state.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        boolean throwException = randomBoolean();
        final ReplicationTask task = maybeTask();
        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testActionWithExceptions",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        ) {

            @Override
            protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
                ActionListener.completeWith(listener, () -> {
                    assertIndexShardCounter(1);
                    assertPhase(task, "replica");
                    if (throwException) {
                        throw new ElasticsearchException("simulated");
                    }
                    return new ReplicaResult();
                });
            }
        };
        try {
            action.handleReplicaRequest(
                new TransportReplicationAction.ConcreteReplicaRequest<>(
                    new Request(shardId),
                    replicaRouting.allocationId().getId(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                ),
                createTransportChannel(new PlainActionFuture<>()),
                task
            );
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("simulated"));
            assertTrue(throwException);
        }
        assertPhase(task, "finished");
        // operation should have finished and counter decreased because no outstanding replica requests
        assertIndexShardCounter(0);
    }

    /**
     * This test ensures that replication operations adhere to the {@link IndexMetadata#SETTING_WAIT_FOR_ACTIVE_SHARDS} setting
     * when the request is using the default value for waitForActiveShards.
     */
    public void testDefaultWaitForActiveShardsUsesIndexSetting() {
        final String indexName = "test";
        final ShardId shardId = new ShardId(indexName, "_na_", 0);

        // test wait_for_active_shards index setting used when the default is set on the request
        int numReplicas = randomIntBetween(0, 5);
        int idxSettingWaitForActiveShards = randomIntBetween(0, numReplicas + 1);
        ClusterState state = stateWithActivePrimary(indexName, randomBoolean(), numReplicas);
        IndexMetadata indexMetadata = state.metadata().index(indexName);
        Settings indexSettings = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey(), Integer.toString(idxSettingWaitForActiveShards))
            .build();
        Metadata.Builder metadataBuilder = Metadata.builder(state.metadata())
            .put(IndexMetadata.builder(indexMetadata).settings(indexSettings).build(), true);
        state = ClusterState.builder(state).metadata(metadataBuilder).build();
        setState(clusterService, state);
        Request request = new Request(shardId).waitForActiveShards(ActiveShardCount.DEFAULT); // set to default so index settings are used
        action.resolveRequest(state.metadata().index(indexName), request);
        assertEquals(ActiveShardCount.from(idxSettingWaitForActiveShards), request.waitForActiveShards());

        // test wait_for_active_shards when default not set on the request (request value should be honored over index setting)
        int requestWaitForActiveShards = randomIntBetween(0, numReplicas + 1);
        request = new Request(shardId).waitForActiveShards(ActiveShardCount.from(requestWaitForActiveShards));
        action.resolveRequest(state.metadata().index(indexName), request);
        assertEquals(ActiveShardCount.from(requestWaitForActiveShards), request.waitForActiveShards());
    }

    /** test that a primary request is rejected if it arrives at a shard with a wrong allocation id or term */
    public void testPrimaryActionRejectsWrongAidOrWrongTerm() throws Exception {
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        setState(clusterService, state(index, true, ShardRoutingState.STARTED));
        final ShardRouting primary = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        final long primaryTerm = clusterService.state().metadata().index(shardId.getIndexName()).primaryTerm(shardId.id());
        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        final boolean wrongAllocationId = randomBoolean();
        final long requestTerm = wrongAllocationId && randomBoolean() ? primaryTerm : primaryTerm + randomIntBetween(1, 10);
        Request request = new Request(shardId).timeout("1ms");
        action.handlePrimaryRequest(
            new TransportReplicationAction.ConcreteShardRequest<>(
                request,
                wrongAllocationId ? "_not_a_valid_aid_" : primary.allocationId().getId(),
                requestTerm
            ),
            createTransportChannel(listener),
            maybeTask()
        );
        try {
            listener.get();
            fail("using a wrong aid didn't fail the operation");
        } catch (ExecutionException execException) {
            Throwable throwable = execException.getCause();
            logger.debug("got exception:", throwable);
            assertTrue(throwable.getClass() + " is not a retry exception", TransportReplicationAction.retryPrimaryException(throwable));
            if (wrongAllocationId) {
                assertThat(
                    throwable.getMessage(),
                    containsString("expected allocation id [_not_a_valid_aid_] but found [" + primary.allocationId().getId() + "]")
                );
            } else {
                assertThat(
                    throwable.getMessage(),
                    containsString(
                        "expected allocation id ["
                            + primary.allocationId().getId()
                            + "] with term ["
                            + requestTerm
                            + "] but found ["
                            + primaryTerm
                            + "]"
                    )
                );
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

        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        Request request = new Request(shardId).timeout("1ms");
        action.handleReplicaRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                request,
                "_not_a_valid_aid_",
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            createTransportChannel(listener),
            maybeTask()
        );
        try {
            listener.get();
            fail("using a wrong aid didn't fail the operation");
        } catch (ExecutionException execException) {
            Throwable throwable = execException.getCause();
            if (TransportReplicationAction.retryPrimaryException(throwable) == false) {
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
        final long primaryTerm = state.metadata().index(shardId.getIndexName()).primaryTerm(shardId.id());
        // simulate execution of the node holding the replica
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.nodes()).localNodeId(replica.currentNodeId())).build();
        setState(clusterService, state);
        AtomicBoolean throwException = new AtomicBoolean(true);
        final ReplicationTask task = maybeTask();
        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testActionWithExceptions",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        ) {
            @Override
            protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
                ActionListener.completeWith(listener, () -> {
                    assertPhase(task, "replica");
                    if (throwException.get()) {
                        throw new RetryOnReplicaException(shardId, "simulation");
                    }
                    return new ReplicaResult();
                });
            }
        };
        final PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        final Request request = new Request(shardId);
        final long checkpoint = randomNonNegativeLong();
        final long maxSeqNoOfUpdatesOrDeletes = randomNonNegativeLong();
        action.handleReplicaRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                request,
                replica.allocationId().getId(),
                primaryTerm,
                checkpoint,
                maxSeqNoOfUpdatesOrDeletes
            ),
            createTransportChannel(listener),
            task
        );
        if (listener.isDone()) {
            listener.get(); // fail with the exception if there
            fail("listener shouldn't be done");
        }

        // no retry yet
        List<CapturingTransport.CapturedRequest> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear()
            .get(replica.currentNodeId());
        assertThat(capturedRequests, nullValue());

        // release the waiting
        throwException.set(false);
        setState(clusterService, state);

        capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear().get(replica.currentNodeId());
        assertThat(capturedRequests, notNullValue());
        assertThat(capturedRequests.size(), equalTo(1));
        final CapturingTransport.CapturedRequest capturedRequest = capturedRequests.get(0);
        assertThat(capturedRequest.action(), equalTo("internal:testActionWithExceptions[r]"));
        assertThat(capturedRequest.request(), instanceOf(TransportReplicationAction.ConcreteReplicaRequest.class));
        assertThat(
            ((TransportReplicationAction.ConcreteReplicaRequest) capturedRequest.request()).getGlobalCheckpoint(),
            equalTo(checkpoint)
        );
        assertThat(
            ((TransportReplicationAction.ConcreteReplicaRequest) capturedRequest.request()).getMaxSeqNoOfUpdatesOrDeletes(),
            equalTo(maxSeqNoOfUpdatesOrDeletes)
        );
        assertConcreteShardRequest(capturedRequest.request(), request, replica.allocationId());
    }

    public void testRetryOnReplicaWithRealTransport() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ClusterState initialState = state(shardId.getIndexName(), true, ShardRoutingState.STARTED, ShardRoutingState.STARTED);
        final ShardRouting replica = initialState.getRoutingTable().shardRoutingTable(shardId).replicaShards().get(0);
        final long primaryTerm = initialState.metadata().index(shardId.getIndexName()).primaryTerm(shardId.id());
        // simulate execution of the node holding the replica
        final ClusterState stateWithNodes = ClusterState.builder(initialState)
            .nodes(DiscoveryNodes.builder(initialState.nodes()).localNodeId(replica.currentNodeId()))
            .build();
        setState(clusterService, stateWithNodes);
        AtomicBoolean throwException = new AtomicBoolean(true);
        final ReplicationTask task = maybeTask();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        final Transport transport = new Netty4Transport(
            Settings.EMPTY,
            TransportVersion.current(),
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(Settings.EMPTY)
        );
        transportService = new MockTransportService(
            Settings.EMPTY,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        AtomicBoolean calledSuccessfully = new AtomicBoolean(false);
        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testActionWithExceptions",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        ) {
            @Override
            protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
                ActionListener.completeWith(listener, () -> {
                    assertPhase(task, "replica");
                    if (throwException.get()) {
                        throw new RetryOnReplicaException(shardId, "simulation");
                    }
                    calledSuccessfully.set(true);
                    return new ReplicaResult();
                });
            }
        };
        final PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        final Request request = new Request(shardId);
        final long checkpoint = randomNonNegativeLong();
        final long maxSeqNoOfUpdates = randomNonNegativeLong();
        action.handleReplicaRequest(
            new TransportReplicationAction.ConcreteReplicaRequest<>(
                request,
                replica.allocationId().getId(),
                primaryTerm,
                checkpoint,
                maxSeqNoOfUpdates
            ),
            createTransportChannel(listener),
            task
        );
        if (listener.isDone()) {
            listener.get(); // fail with the exception if there
            fail("listener shouldn't be done");
        }

        // release the waiting
        throwException.set(false);
        // publish a new state (same as the old state with the version incremented)
        setState(clusterService, stateWithNodes);

        // Assert that the request was retried, this time successful
        assertTrue("action should have been successfully called on retry but was not", calledSuccessfully.get());
        transportService.stop();
    }

    public void testIsRetryableClusterBlockException() {
        final TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testIsRetryableClusterBlockException",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );
        assertFalse(
            TransportReplicationAction.isRetryableClusterBlockException(randomRetryPrimaryException(new ShardId("index", "_na_", 0)))
        );

        final boolean retryable = randomBoolean();
        ClusterBlock randomBlock = new ClusterBlock(
            randomIntBetween(1, 16),
            "test",
            retryable,
            randomBoolean(),
            randomBoolean(),
            randomFrom(RestStatus.values()),
            EnumSet.of(randomFrom(ClusterBlockLevel.values()))
        );
        assertEquals(
            retryable,
            TransportReplicationAction.isRetryableClusterBlockException(new ClusterBlockException(singleton(randomBlock)))
        );
    }

    private void assertConcreteShardRequest(TransportRequest capturedRequest, Request expectedRequest, AllocationId expectedAllocationId) {
        final TransportReplicationAction.ConcreteShardRequest<?> concreteShardRequest = (TransportReplicationAction.ConcreteShardRequest<
            ?>) capturedRequest;
        assertThat(concreteShardRequest.getRequest(), equalTo(expectedRequest));
        assertThat(((Request) concreteShardRequest.getRequest()).isRetrySet.get(), equalTo(true));
        assertThat(concreteShardRequest.getTargetAllocationID(), equalTo(expectedAllocationId.getId()));
    }

    private void assertIndexShardCounter(int expected) {
        assertThat(count.get(), equalTo(expected));
    }

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicBoolean isRelocated = new AtomicBoolean(false);

    private final AtomicBoolean isPrimaryMode = new AtomicBoolean(true);

    /**
     * Sometimes build a ReplicationTask for tracking the phase of the
     * TransportReplicationAction. Since TransportReplicationAction has to work
     * if the task as null just as well as if it is supplied this returns null
     * half the time.
     */
    private ReplicationTask maybeTask() {
        return random().nextBoolean() ? new ReplicationTask(0, null, null, null, null, null) : null;
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
        AtomicBoolean processedOnPrimary = new AtomicBoolean();
        AtomicInteger processedOnReplicas = new AtomicInteger();
        AtomicBoolean isRetrySet = new AtomicBoolean(false);

        Request(StreamInput in) throws IOException {
            super(in);
        }

        Request(@Nullable ShardId shardId) {
            super(shardId);
            this.waitForActiveShards = ActiveShardCount.NONE;
            // keep things simple
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
        TestResponse(StreamInput in) throws IOException {
            super(in);
        }

        TestResponse() {
            setShardInfo(new ShardInfo());
        }
    }

    private class TestAction extends TransportReplicationAction<Request, Request, TestResponse> {

        TestAction(
            Settings settings,
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ShardStateAction shardStateAction,
            ThreadPool threadPool
        ) {
            this(settings, actionName, transportService, clusterService, shardStateAction, threadPool, mockIndicesService(clusterService));
        }

        TestAction(
            Settings settings,
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ShardStateAction shardStateAction,
            ThreadPool threadPool,
            IndicesService indicesService
        ) {
            super(
                settings,
                actionName,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                new ActionFilters(new HashSet<>()),
                Request::new,
                Request::new,
                ThreadPool.Names.SAME,
                false,
                forceExecute
            );
        }

        @Override
        protected TestResponse newResponseInstance(StreamInput in) throws IOException {
            return new TestResponse(in);
        }

        @Override
        protected void shardOperationOnPrimary(
            Request shardRequest,
            IndexShard primary,
            ActionListener<PrimaryResult<Request, TestResponse>> listener
        ) {
            boolean executedBefore = shardRequest.processedOnPrimary.getAndSet(true);
            assert executedBefore == false : "request has already been executed on the primary";
            listener.onResponse(new PrimaryResult<>(shardRequest, new TestResponse()));
        }

        @Override
        protected void shardOperationOnReplica(Request request, IndexShard replica, ActionListener<ReplicaResult> listener) {
            request.processedOnReplicas.incrementAndGet();
            listener.onResponse(new ReplicaResult());
        }
    }

    private IndicesService mockIndicesService(ClusterService clusterService) {
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            final IndexMetadata indexSafe = state.metadata().getIndexSafe(index);
            return mockIndexService(indexSafe, clusterService);
        });
        when(indicesService.indexService(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            if (state.metadata().hasIndex(index.getName())) {
                return mockIndexService(clusterService.state().metadata().getIndexSafe(index), clusterService);
            } else {
                return null;
            }
        });
        return indicesService;
    }

    private IndexService mockIndexService(final IndexMetadata indexMetadata, ClusterService clusterService) {
        final IndexService indexService = mock(IndexService.class);
        when(indexService.getShard(anyInt())).then(invocation -> {
            int shard = (Integer) invocation.getArguments()[0];
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), shard);
            if (shard > indexMetadata.getNumberOfShards()) {
                throw new ShardNotFoundException(shardId);
            }
            return mockIndexShard(shardId, clusterService);
        });
        return indexService;
    }

    @SuppressWarnings("unchecked")
    private IndexShard mockIndexShard(ShardId shardId, ClusterService clusterService) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        doAnswer(invocation -> {
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
            if (isPrimaryMode.get()) {
                count.incrementAndGet();
                callback.onResponse(count::decrementAndGet);

            } else {
                callback.onFailure(new ShardNotInPrimaryModeException(shardId, IndexShardState.STARTED));
            }
            return null;
        }).when(indexShard).acquirePrimaryOperationPermit(any(ActionListener.class), anyString(), eq(forceExecute));
        when(indexShard.isPrimaryMode()).thenAnswer(invocation -> isPrimaryMode.get());
        doAnswer(invocation -> {
            long term = (Long) invocation.getArguments()[0];
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[3];
            final long primaryTerm = indexShard.getPendingPrimaryTerm();
            if (term < primaryTerm) {
                throw new IllegalArgumentException(
                    Strings.format("%s operation term [%d] is too old (current [%d])", shardId, term, primaryTerm)
                );
            }
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquireReplicaOperationPermit(anyLong(), anyLong(), anyLong(), any(ActionListener.class), anyString());
        when(indexShard.getActiveOperationsCount()).thenAnswer(i -> count.get());

        when(indexShard.routingEntry()).thenAnswer(invocationOnMock -> {
            final ClusterState state = clusterService.state();
            final RoutingNode node = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
            final ShardRouting routing = node.getByShardId(shardId);
            if (routing == null) {
                throw new ShardNotFoundException(shardId, "shard is no longer assigned to current node");
            }
            return routing;
        });
        when(indexShard.isRelocatedPrimary()).thenAnswer(invocationOnMock -> isRelocated.get());
        doThrow(new AssertionError("failed shard is not supported")).when(indexShard).failShard(anyString(), any(Exception.class));
        when(indexShard.getPendingPrimaryTerm()).thenAnswer(
            i -> clusterService.state().metadata().getIndexSafe(shardId.getIndex()).primaryTerm(shardId.id())
        );

        ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
        when(indexShard.getReplicationGroup()).thenReturn(replicationGroup);
        return indexShard;
    }

    /**
     * Transport channel that is needed for replica operation testing.
     */
    public TransportChannel createTransportChannel(final PlainActionFuture<TransportResponse> listener) {
        return new TestTransportChannel(listener);
    }

}
