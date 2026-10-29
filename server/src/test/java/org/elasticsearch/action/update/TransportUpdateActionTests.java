/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.update;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.action.DocWriteResponse.Result.NOT_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

// This test suite mocks out a fair amount of TransportUpdateAction behavior because it was ported from a
// more generic superclass (TransportInstancesSingleOperationActionTests) that only had a single subclass.
// It deserves more coverage than it currently has.
public class TransportUpdateActionTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;

    private ProjectId projectId;

    private ClusterService clusterService;
    private ProjectResolver projectResolver;
    private CapturingTransport transport;
    private TransportService transportService;

    private static UpdateResponse RESPONSE = new UpdateResponse(new ShardId("index", "index_uuid", 0), "id", -2, 0, 0, NOT_FOUND);

    class TestTransportUpdateAction extends TransportUpdateAction {
        TestTransportUpdateAction() {
            this(new MyResolver());
        }

        private TestTransportUpdateAction(IndexNameExpressionResolver indexNameExpressionResolver) {
            super(
                THREAD_POOL,
                TransportUpdateActionTests.this.clusterService,
                TransportUpdateActionTests.this.projectResolver,
                TransportUpdateActionTests.this.transportService,
                mock(UpdateHelper.class),
                new ActionFilters(new HashSet<>()),
                indexNameExpressionResolver,
                mock(IndicesService.class),
                new AutoCreateIndex(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    indexNameExpressionResolver,
                    EmptySystemIndices.INSTANCE
                ),
                mock(NodeClient.class)
            );
        }

        @Override
        protected Executor executor(ShardId shardId) {
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        }

        @Override
        protected void resolveRequest(ProjectState state, UpdateRequest request) {}
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

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportUpdateActionTests.class.getSimpleName());
    }

    @Before
    public void initServices() throws Exception {
        projectId = randomProjectIdOrDefault();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        projectResolver = TestProjectResolvers.singleProject(projectId);
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
    }

    @After
    public void closeServices() throws Exception {
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        UpdateRequest request = new UpdateRequest();
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(
            clusterService,
            ClusterState.builder(clusterService.state()).putProjectMetadata(ProjectMetadata.builder(projectId)).blocks(block)
        );
        try {
            new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
            listener.get();
            fail("expected ClusterBlockException");
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, ClusterBlockException.class) == null) {
                logger.info("expected ClusterBlockException  but got ", e);
                fail("expected ClusterBlockException");
            }
        }
    }

    public void testBasicRequestWorks() throws InterruptedException, ExecutionException, TimeoutException {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testFailureWithoutRetry() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        // this should not trigger retry or anything and the listener should report exception immediately
        transport.handleRemoteError(
            requestId,
            new TransportException("a generic transport exception", new Exception("generic test exception"))
        );

        try {
            // result should return immediately
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, TransportException.class) == null) {
                logger.info("expected TransportException  but got ", t);
                fail("expected and TransportException");
            }
        }
    }

    public void testSuccessAfterRetryWithClusterStateUpdate() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.INITIALIZING));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        // this should fail because primary not initialized
        assertThat(transport.capturedRequests().length, equalTo(0));
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        // this time it should work
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testSuccessAfterRetryWithExceptionFromTransport() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        boolean local = randomBoolean();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        // trigger cluster state observer
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", local, ShardRoutingState.STARTED));
        assertThat(transport.capturedRequests().length, equalTo(1));
        transport.handleResponse(transport.capturedRequests()[0].requestId(), RESPONSE);
        listener.get();
    }

    public void testRetryOfAnAlreadyTimedOutRequest() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id").timeout(new TimeValue(0, TimeUnit.MILLISECONDS));
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        DiscoveryNode node = clusterService.state().getNodes().getLocalNode();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));

        // wait until the timeout was triggered and we actually tried to send for the second time
        assertBusy(() -> assertThat(transport.capturedRequests().length, equalTo(1)));

        // let it fail the second time too
        requestId = transport.capturedRequests()[0].requestId();
        transport.handleLocalError(requestId, new ConnectTransportException(node, "test exception"));
        try {
            // result should return immediately
            assertTrue(listener.isDone());
            listener.get();
            fail("this should fail with a transport exception");
        } catch (ExecutionException t) {
            if (ExceptionsHelper.unwrap(t, ConnectTransportException.class) == null) {
                logger.info("expected ConnectTransportException  but got ", t);
                fail("expected and ConnectTransportException");
            }
        }
    }

    public void testUnresolvableRequestDoesNotHang() throws InterruptedException, ExecutionException, TimeoutException {
        final var action = new TestTransportUpdateAction() {
            @Override
            protected void resolveRequest(ProjectState state, UpdateRequest request) {
                throw new IllegalStateException("request cannot be resolved");
            }
        };
        UpdateRequest request = new UpdateRequest().index("test");
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));
        action.new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(0));
        try {
            listener.get();
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, IllegalStateException.class) == null) {
                logger.info("expected IllegalStateException  but got ", e);
                fail("expected and IllegalStateException");
            }
        }
    }

    /**
     * A request that times out while the primary is being restored by a restore that reports shard restoring fails with
     * {@link ShardRestoringException} for the restore, rather than the generic {@link UnavailableShardsException}. The request never
     * reaches the transport because the primary is not active.
     */
    public void testTimeoutFailsWithShardRestoringExceptionWhenRestoreReportsShardRestoring() throws Exception {
        final String restoreUuid = randomUUID();
        UpdateRequest request = new UpdateRequest("test", "id").timeout(TimeValue.ZERO);
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, stateWithPrimaryBeingRestored("test", restoreUuid, true));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();

        // the timeout fires on another thread, so wait for the listener to be failed
        final ExecutionException e = expectThrows(ExecutionException.class, listener::get);
        final ShardRestoringException cause = (ShardRestoringException) ExceptionsHelper.unwrap(e, ShardRestoringException.class);
        assertNotNull("expected ShardRestoringException but got " + e, cause);
        assertThat(cause.recoveryId(), equalTo(restoreUuid));
        assertThat(cause.getMetadata("es.index"), equalTo(List.of("test")));
        assertThat(cause.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(transport.capturedRequests().length, equalTo(0));
    }

    /**
     * A restore that did not ask for shard restoring to be reported keeps the generic {@link UnavailableShardsException} on timeout.
     */
    public void testTimeoutFailsWithUnavailableShardsWhenRestoreDoesNotReportShardRestoring() throws Exception {
        UpdateRequest request = new UpdateRequest("test", "id").timeout(TimeValue.ZERO);
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, stateWithPrimaryBeingRestored("test", randomUUID(), false));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();

        final ExecutionException e = expectThrows(ExecutionException.class, listener::get);
        assertNotNull("expected UnavailableShardsException but got " + e, ExceptionsHelper.unwrap(e, UnavailableShardsException.class));
        assertNull("unexpected ShardRestoringException in " + e, ExceptionsHelper.unwrap(e, ShardRestoringException.class));
        assertThat(transport.capturedRequests().length, equalTo(0));
    }

    /**
     * Unlike the replication actions, an update does not fail fast: it keeps waiting for the primary until its timeout expires even if
     * the restore reports shard restoring, and only then reports {@link ShardRestoringException}.
     */
    public void testRequestWaitsForPrimaryBeingRestoredUntilTimeout() {
        UpdateRequest request = new UpdateRequest("test", "id").timeout(TimeValue.timeValueMinutes(1));
        request.shardId = new ShardId("test", "_na_", 0);
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, stateWithPrimaryBeingRestored("test", randomUUID(), true));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();

        assertFalse("a request for a primary being restored should keep waiting until its timeout", listener.isDone());
        assertThat(transport.capturedRequests().length, equalTo(0));
    }

    /**
     * A coordinating node that has not applied the restore yet still sees an active primary and sends the update. The node holding the
     * shard answers with a shard-level error, and if the coordinator never learns of the restore the request fails with that error
     * rather than with {@link ShardRestoringException}.
     */
    public void testCoordinatorThatNeverLearnsOfRestoreFailsWithShardLevelError() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        UpdateRequest request = new UpdateRequest("test", "id").timeout(TimeValue.ZERO);
        request.shardId = shardId;
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();
        transport.handleRemoteError(requestId, shardRecoveringException(shardId));

        // the timeout fires and the last attempt runs against the same stale state, so the update is sent again
        assertBusy(() -> assertThat(transport.capturedRequests().length, equalTo(1)));
        transport.handleRemoteError(transport.capturedRequests()[0].requestId(), shardRecoveringException(shardId));

        final ExecutionException e = expectThrows(ExecutionException.class, listener::get);
        assertNotNull(
            "expected IllegalIndexShardStateException but got " + e,
            ExceptionsHelper.unwrap(e, IllegalIndexShardStateException.class)
        );
        assertNull("unexpected ShardRestoringException in " + e, ExceptionsHelper.unwrap(e, ShardRestoringException.class));
    }

    /**
     * A coordinating node that applies the restore while the update is in flight reports {@link ShardRestoringException} once the node
     * holding the shard answers with a shard-level error: the retry runs against the newer state, which shows the restore.
     */
    public void testCoordinatorThatLearnsOfRestoreWhileUpdateIsInFlightFailsWithShardRestoringException() throws Exception {
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final String restoreUuid = randomUUID();
        UpdateRequest request = new UpdateRequest("test", "id").timeout(TimeValue.ZERO);
        request.shardId = shardId;
        PlainActionFuture<UpdateResponse> listener = new PlainActionFuture<>();
        setState(clusterService, ClusterStateCreationUtils.state(projectId, "test", randomBoolean(), ShardRoutingState.STARTED));

        new TestTransportUpdateAction().new AsyncSingleAction(request, listener).start();
        assertThat(transport.capturedRequests().length, equalTo(1));
        long requestId = transport.capturedRequests()[0].requestId();
        transport.clear();

        // the restore reaches the coordinator after the update was sent, but before the shard-level error comes back
        setState(clusterService, stateWithPrimaryBeingRestored("test", restoreUuid, true));
        transport.handleRemoteError(requestId, shardRecoveringException(shardId));

        final ExecutionException e = expectThrows(ExecutionException.class, listener::get);
        final ShardRestoringException cause = (ShardRestoringException) ExceptionsHelper.unwrap(e, ShardRestoringException.class);
        assertNotNull("expected ShardRestoringException but got " + e, cause);
        assertThat(cause.recoveryId(), equalTo(restoreUuid));
        assertThat(transport.capturedRequests().length, equalTo(0));
    }

    private static IllegalIndexShardStateException shardRecoveringException(ShardId shardId) {
        return new IllegalIndexShardStateException(shardId, IndexShardState.RECOVERING, "operation only allowed when started");
    }

    /**
     * A cluster state in which the only shard's primary is initializing from a snapshot, with a matching started restore.
     */
    private ClusterState stateWithPrimaryBeingRestored(String index, String restoreUuid, boolean reportShardRestoring) {
        final ClusterState state = ClusterStateCreationUtils.state(projectId, index, randomBoolean(), ShardRoutingState.INITIALIZING);
        final ShardRouting initializing = state.routingTable(projectId).index(index).shard(0).primaryShard();
        final ShardId shardId = initializing.shardId();
        final Snapshot snapshot = new Snapshot(projectId, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));
        final ShardRouting restoring = TestShardRouting.shardRoutingBuilder(
            shardId,
            initializing.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        )
            .withRecoverySource(
                new RecoverySource.SnapshotRecoverySource(
                    restoreUuid,
                    snapshot,
                    IndexVersion.current(),
                    new IndexId(index, shardId.getIndex().getUUID())
                )
            )
            .build();
        final RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(
            new RestoreInProgress.Entry(
                restoreUuid,
                snapshot,
                RestoreInProgress.State.STARTED,
                false,
                List.of(index),
                Map.of(shardId, new RestoreInProgress.ShardRestoreStatus(initializing.currentNodeId())),
                reportShardRestoring
            )
        ).build();
        return ClusterState.builder(state)
            .putRoutingTable(
                projectId,
                RoutingTable.builder().add(IndexRoutingTable.builder(shardId.getIndex()).addShard(restoring)).build()
            )
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();
    }
}
