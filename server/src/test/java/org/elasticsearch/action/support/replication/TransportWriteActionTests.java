/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationOperation.ReplicaResponse;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ReplicationGroup;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportWriteActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    private final ProjectId projectId = randomProjectIdOrDefault();
    private ClusterService clusterService;
    private IndexShard indexShard;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("ShardReplicationTests");
    }

    @Before
    public void initCommonMocks() {
        indexShard = mock(IndexShard.class);
        clusterService = createClusterService(threadPool);
        when(indexShard.refresh(any())).thenReturn(new Engine.RefreshResult(true, randomNonNegativeLong(), 1));
        ReplicationGroup replicationGroup = mock(ReplicationGroup.class);
        when(indexShard.getReplicationGroup()).thenReturn(replicationGroup);
        when(replicationGroup.getReplicationTargets()).thenReturn(Collections.emptyList());
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

    <T> void assertListenerThrows(String msg, PlainActionFuture<T> listener, Class<?> klass) throws InterruptedException {
        try {
            listener.get();
            fail(msg);
        } catch (ExecutionException ex) {
            assertThat(ex.getCause(), instanceOf(klass));
        }
    }

    public void testPrimaryNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(listener.map(ignore -> result.replicationResponse));
            assertNotNull(listener.response);
            assertNull(listener.failure);
            verify(indexShard, never()).refresh(any());
            verify(indexShard, never()).addRefreshListener(any(), any());
        }));
    }

    public void testReplicaNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = new PlainActionFuture<>();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<ActionResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(listener.map(ignore -> ActionResponse.Empty.INSTANCE));
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(listener.map(ignore -> result.replicationResponse));

            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArgumentCaptor<ActionListener<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) ActionListener.class);
            verify(testAction.postWriteRefresh).refreshShard(
                eq(RefreshPolicy.IMMEDIATE),
                eq(indexShard),
                any(),
                refreshListener.capture(),
                eq(request.timeout())
            );

            // Now we can fire the listener manually and we'll get a response
            refreshListener.getValue().onResponse(true);
            assertEquals(true, listener.response.forcedRefresh);
        }));
    }

    public void testReplicaImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = new PlainActionFuture<>();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<ActionResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(listener.map(ignore -> ActionResponse.Empty.INSTANCE));
        assertNull(listener.response); // Haven't responded yet
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<ActionListener<Engine.RefreshResult>> refreshListener = ArgumentCaptor.forClass((Class) ActionListener.class);
        verify(indexShard).externalRefresh(eq(PostWriteRefresh.FORCED_REFRESH_AFTER_INDEX), refreshListener.capture());
        verify(indexShard, never()).addRefreshListener(any(), any());
        // Fire the listener manually
        refreshListener.getValue().onResponse(new Engine.RefreshResult(randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong()));
        assertNotNull(listener.response);
        assertNull(listener.failure);
    }

    public void testPrimaryWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);

        TestAction testAction = new TestAction();
        testAction.dispatchedShardOperationOnPrimary(request, indexShard, ActionTestUtils.assertNoFailureListener(result -> {
            CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
            result.runPostReplicationActions(listener.map(ignore -> result.replicationResponse));
            assertNull(listener.response); // Haven't really responded yet

            @SuppressWarnings({ "unchecked", "rawtypes" })
            ArgumentCaptor<ActionListener<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) ActionListener.class);
            verify(testAction.postWriteRefresh).refreshShard(
                eq(RefreshPolicy.WAIT_UNTIL),
                eq(indexShard),
                any(),
                refreshListener.capture(),
                eq(request.timeout())
            );

            // Now we can fire the listener manually and we'll get a response
            boolean forcedRefresh = randomBoolean();
            refreshListener.getValue().onResponse(forcedRefresh);
            assertEquals(forcedRefresh, listener.response.forcedRefresh);
        }));
    }

    public void testReplicaWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        TestAction testAction = new TestAction();
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = new PlainActionFuture<>();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<ActionResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(listener.map(ignore -> ActionResponse.Empty.INSTANCE));
        assertNull(listener.response); // Haven't responded yet
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(indexShard, never()).refresh(any());
        verify(indexShard).addRefreshListener(any(), refreshListener.capture());

        // Now we can fire the listener manually and we'll get a response
        boolean forcedRefresh = randomBoolean();
        refreshListener.getValue().accept(forcedRefresh);
        assertNotNull(listener.response);
        assertNull(listener.failure);
    }

    public void testDocumentFailureInShardOperationOnPrimary() {
        final var listener = SubscribableListener.<Exception>newForked(
            l -> new TestAction(true, randomBoolean()).dispatchedShardOperationOnPrimary(
                new TestRequest(),
                indexShard,
                ActionTestUtils.assertNoSuccessListener(l::onResponse)
            )
        );
        assertTrue(listener.isDone());
        assertEquals("simulated", asInstanceOf(RuntimeException.class, safeAwait(listener)).getMessage());
    }

    public void testDocumentFailureInShardOperationOnReplica() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(randomBoolean(), true);
        final PlainActionFuture<TransportReplicationAction.ReplicaResult> future = new PlainActionFuture<>();
        testAction.dispatchedShardOperationOnReplica(request, indexShard, future);
        final TransportReplicationAction.ReplicaResult result = future.actionGet();
        CapturingActionListener<ActionResponse.Empty> listener = new CapturingActionListener<>();
        result.runPostReplicaActions(listener.map(ignore -> ActionResponse.Empty.INSTANCE));
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    public void testReplicaProxy() throws InterruptedException, ExecutionException {
        CapturingTransport transport = new CapturingTransport();
        TransportService transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        ShardStateAction shardStateAction = new ShardStateAction(clusterService, transportService, null, null, threadPool);
        TestAction action = new TestAction(
            Settings.EMPTY,
            "internal:testAction",
            transportService,
            clusterService,
            shardStateAction,
            threadPool
        );
        final String index = "test";
        final ShardId shardId = new ShardId(index, "_na_", 0);
        ClusterState state = ClusterStateCreationUtils.stateWithActivePrimary(projectId, index, true, 1 + randomInt(3), randomInt(2));
        logger.info("using state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        final long primaryTerm = state.metadata().getProject(projectId).index(index).primaryTerm(0);
        ReplicationOperation.Replicas<TestRequest> proxy = action.newReplicasProxy();

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
            new TestRequest(),
            primaryTerm,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            listener
        );
        assertTrue(listener.isDone());
        assertListenerThrows("non existent node should throw a NoNodeAvailableException", listener, NoNodeAvailableException.class);

        final IndexShardRoutingTable shardRoutings = state.routingTable(projectId).shardRoutingTable(shardId);
        final ShardRouting replica = randomFrom(shardRoutings.replicaShards().stream().filter(ShardRouting::assignedToNode).toList());
        listener = new PlainActionFuture<>();
        proxy.performOn(replica, new TestRequest(), primaryTerm, randomNonNegativeLong(), randomNonNegativeLong(), listener);
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
        // A write replication action proxy should fail the shard
        assertEquals(1, shardFailedRequests.length);
        CapturingTransport.CapturedRequest shardFailedRequest = shardFailedRequests[0];
        ShardStateAction.FailedShardEntry shardEntry = (ShardStateAction.FailedShardEntry) shardFailedRequest.request();
        // the shard the request was sent to and the shard to be failed should be the same
        assertEquals(shardEntry.getShardId(), replica.shardId());
        assertEquals(shardEntry.getAllocationId(), replica.allocationId().getId());
        if (randomBoolean()) {
            // simulate success
            transport.handleResponse(shardFailedRequest.requestId(), ActionResponse.Empty.INSTANCE);
            assertTrue(success.get());
            assertNull(failure.get());
        } else if (randomBoolean()) {
            // simulate the primary has been demoted
            transport.handleRemoteError(
                shardFailedRequest.requestId(),
                new ShardStateAction.NoLongerPrimaryShardException(replica.shardId(), "shard-failed-test")
            );
            assertFalse(success.get());
            assertNotNull(failure.get());
        } else {
            // simulated a node closing exception
            transport.handleRemoteError(shardFailedRequest.requestId(), new NodeClosedException(state.nodes().getLocalNode()));
            assertFalse(success.get());
            assertNotNull(failure.get());
        }
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestRequest, TestResponse> {

        private final boolean withDocumentFailureOnPrimary;
        private final boolean withDocumentFailureOnReplica;

        private final PostWriteRefresh postWriteRefresh = mock(PostWriteRefresh.class);

        protected TestAction() {
            this(false, false);
        }

        protected TestAction(boolean withDocumentFailureOnPrimary, boolean withDocumentFailureOnReplica) {
            super(
                Settings.EMPTY,
                "internal:test",
                new TransportService(
                    Settings.EMPTY,
                    mock(Transport.class),
                    TransportWriteActionTests.threadPool,
                    TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                    x -> null,
                    null,
                    Collections.emptySet()
                ),
                TransportWriteActionTests.this.clusterService,
                null,
                TransportWriteActionTests.threadPool,
                null,
                new ActionFilters(new HashSet<>()),
                TestRequest::new,
                TestRequest::new,
                (service, ignore) -> EsExecutors.DIRECT_EXECUTOR_SERVICE,
                PrimaryActionExecution.RejectOnOverload,
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                TestProjectResolvers.singleProject(projectId),
                ReplicaActionExecution.SubjectToCircuitBreaker
            );
            this.withDocumentFailureOnPrimary = withDocumentFailureOnPrimary;
            this.withDocumentFailureOnReplica = withDocumentFailureOnReplica;
        }

        protected TestAction(
            Settings settings,
            String actionName,
            TransportService transportService,
            ClusterService clusterService,
            ShardStateAction shardStateAction,
            ThreadPool threadPool
        ) {
            super(
                settings,
                actionName,
                transportService,
                clusterService,
                mockIndicesService(clusterService),
                threadPool,
                shardStateAction,
                new ActionFilters(new HashSet<>()),
                TestRequest::new,
                TestRequest::new,
                (service, ignore) -> EsExecutors.DIRECT_EXECUTOR_SERVICE,
                PrimaryActionExecution.RejectOnOverload,
                new IndexingPressure(settings),
                EmptySystemIndices.INSTANCE,
                TestProjectResolvers.singleProject(projectId),
                ReplicaActionExecution.SubjectToCircuitBreaker
            );
            this.withDocumentFailureOnPrimary = false;
            this.withDocumentFailureOnReplica = false;
        }

        @Override
        protected TestResponse newResponseInstance(StreamInput in) throws IOException {
            return new TestResponse();
        }

        @Override
        protected void dispatchedShardOperationOnPrimary(
            TestRequest request,
            IndexShard primary,
            ActionListener<PrimaryResult<TestRequest, TestResponse>> listener
        ) {
            ActionListener.completeWith(listener, () -> {
                if (withDocumentFailureOnPrimary) {
                    throw new RuntimeException("simulated");
                } else {
                    return new WritePrimaryResult<>(
                        request,
                        new TestResponse(),
                        Translog.Location.EMPTY,
                        primary,
                        logger,
                        postWriteRefresh
                    );
                }
            });
        }

        @Override
        protected void dispatchedShardOperationOnReplica(TestRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
            ActionListener.completeWith(listener, () -> {
                final WriteReplicaResult<TestRequest> replicaResult;
                if (withDocumentFailureOnReplica) {
                    replicaResult = new WriteReplicaResult<>(request, null, new RuntimeException("simulated"), replica, logger);
                } else {
                    replicaResult = new WriteReplicaResult<>(request, Translog.Location.EMPTY, null, replica, logger);
                }
                return replicaResult;
            });
        }
    }

    final IndexService mockIndexService(final IndexMetadata indexMetadata, ClusterService clusterService) {
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

    final IndicesService mockIndicesService(ClusterService clusterService) {
        final IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.indexServiceSafe(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            final IndexMetadata indexSafe = state.metadata().getProject(projectId).getIndexSafe(index);
            return mockIndexService(indexSafe, clusterService);
        });
        when(indicesService.indexService(any(Index.class))).then(invocation -> {
            Index index = (Index) invocation.getArguments()[0];
            final ClusterState state = clusterService.state();
            if (state.metadata().getProject(projectId).hasIndex(index.getName())) {
                return mockIndexService(clusterService.state().metadata().getProject(projectId).getIndexSafe(index), clusterService);
            } else {
                return null;
            }
        });
        return indicesService;
    }

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicBoolean isRelocated = new AtomicBoolean(false);

    private IndexShard mockIndexShard(ShardId shardId, ClusterService clusterService) {
        final IndexShard indexShard = mock(IndexShard.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[0];
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquirePrimaryOperationPermit(anyActionListener(), any(Executor.class), any());
        doAnswer(invocation -> {
            long term = (Long) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<Releasable> callback = (ActionListener<Releasable>) invocation.getArguments()[1];
            final long primaryTerm = indexShard.getPendingPrimaryTerm();
            if (term < primaryTerm) {
                throw new IllegalArgumentException(
                    Strings.format("%s operation term [%d] is too old (current [%d])", shardId, term, primaryTerm)
                );
            }
            count.incrementAndGet();
            callback.onResponse(count::decrementAndGet);
            return null;
        }).when(indexShard).acquireReplicaOperationPermit(anyLong(), anyLong(), anyLong(), anyActionListener(), any(Executor.class));
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
            i -> clusterService.state().metadata().getProject(projectId).getIndexSafe(shardId.getIndex()).primaryTerm(shardId.id())
        );
        return indexShard;
    }

    private static class TestRequest extends ReplicatedWriteRequest<TestRequest> {
        TestRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestRequest() {
            super(new ShardId("test", "test", 1));
        }

        @Override
        public String toString() {
            return "TestRequest{}";
        }
    }

    private static class TestResponse extends ReplicationResponse implements WriteResponse {
        boolean forcedRefresh;

        @Override
        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }
    }

    private static class CapturingActionListener<R> implements ActionListener<R> {
        private R response;
        private Exception failure;

        @Override
        public void onResponse(R response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception failure) {
            this.failure = failure;
        }
    }
}
