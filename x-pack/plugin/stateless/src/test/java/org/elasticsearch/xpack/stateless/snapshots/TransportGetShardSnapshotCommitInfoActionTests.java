/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractThrottledTaskRunnerHelper;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetShardSnapshotCommitInfoActionTests extends ESTestCase {

    private int maxInFlightSendRequests;
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;
    private TransportGetShardSnapshotCommitInfoAction action;
    private SnapshotsCommitService snapshotsCommitService;

    private final ProjectId projectId = ProjectId.DEFAULT;
    private final DiscoveryNode localNode = DiscoveryNodeUtils.create("node0", "node0");
    private final DiscoveryNode remoteNode = DiscoveryNodeUtils.create("node1", "node1");
    private final DiscoveryNode remoteNode2 = DiscoveryNodeUtils.create("node2", "node2");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        transport = new CapturingTransport();
        clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            localNode,
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING)
            )
        );
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            clusterService.getClusterSettings(),
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        snapshotsCommitService = mock(SnapshotsCommitService.class);
        maxInFlightSendRequests = between(2, 10);
        action = new TransportGetShardSnapshotCommitInfoAction(
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            snapshotsCommitService,
            maxInFlightSendRequests
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        clusterService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testShardOperationOnRemoteNode() throws Exception {
        final var shardId = newShardId();
        final var expectedShardStateId = setupShardMocks(shardId);
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);

        // Request is sent to remoteNode
        final var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        assertEquals(remoteNode, capturedRequests[0].node());

        // Simulate shardOperation executing on the remote node
        invokeHandler(capturedRequests[0]);

        final var response = safeGet(future);
        assertEquals(Store.MetadataSnapshot.EMPTY, response.metadataSnapshot());
        assertTrue(response.blobLocations().isEmpty());
        assertEquals(expectedShardStateId, response.shardStateId());
    }

    public void testRetriesWhenPrimaryNotActiveAndSucceeds() throws Exception {
        final var shardId = newShardId();
        final var snapshot = newSnapshot();
        final var expectedShardStateId = setupShardMocks(shardId);
        setupClusterState(remoteNode, ShardRoutingState.INITIALIZING, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, snapshot, future);

        // Primary not active -> retry waits for cluster state change
        assertEquals(0, transport.capturedRequests().length);

        // Primary becomes active on remoteNode -> request is sent
        setupClusterState(remoteNode, ShardRoutingState.STARTED, snapshot, shardId);

        final var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        assertEquals(remoteNode, capturedRequests[0].node());

        invokeHandler(capturedRequests[0]);

        final var response = safeGet(future);
        assertEquals(expectedShardStateId, response.shardStateId());
    }

    public void testTimesOutWaitingForPrimaryToBeActive() {
        final var shardId = newShardId();
        setupClusterState(remoteNode, ShardRoutingState.INITIALIZING, shardId);

        // Set a very short timeout so the observer fires immediately
        clusterService.getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(1))
                    .build()
            );

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);

        // Primary stays inactive — observer times out and the action should fail
        final var e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("timed out waiting for primary shard"));
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testRetriesWhenShardNotFoundAndSucceeds() throws Exception {
        final var shardId = newShardId();
        final var snapshot = newSnapshot();
        final var expectedShardStateId = setupShardMocks(shardId);
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, snapshot, future);

        // Request is sent to remoteNode
        final var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);
        assertEquals(remoteNode, capturedRequests[0].node());

        // Shard relocates to remoteNode2 before the remote processes the request
        setupClusterState(remoteNode2, ShardRoutingState.STARTED, snapshot, shardId);

        // first shardOperation call fails (shard relocated away from remoteNode),
        // second call succeeds (retry on remoteNode2)
        when(snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(eq(shardId), any(Snapshot.class), eq(true), any())).thenThrow(
            new ShardNotFoundException(shardId)
        ).thenReturn(new SnapshotsCommitService.SnapshotCommitInfo(null, Store.MetadataSnapshot.EMPTY, Map.of(), expectedShardStateId));

        // Execute shardOperation with the captured request as if it arrived at the remote node.
        // shardOperation fails because the shard relocated away.
        invokeHandler(capturedRequests[0]);

        // Retry sends request to remoteNode2 (async)
        assertBusy(() -> assertEquals(1, transport.capturedRequests().length));
        final var retryRequests = transport.getCapturedRequestsAndClear();
        assertEquals(remoteNode2, retryRequests[0].node());

        invokeHandler(retryRequests[0]);

        final var response = safeGet(future);
        assertEquals(expectedShardStateId, response.shardStateId());
    }

    public void testRetriesOnTransportExceptionAndSucceeds() throws Exception {
        final var shardId = newShardId();
        final var snapshot = newSnapshot();
        final var expectedShardStateId = setupShardMocks(shardId);
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, snapshot, future);

        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);

        // Move shard to a different node for retry
        setupClusterState(remoteNode2, ShardRoutingState.STARTED, snapshot, shardId);
        final var exception = randomBoolean()
            ? new ConnectTransportException(remoteNode, "connection refused")
            : new NodeClosedException(remoteNode);
        transport.handleRemoteError(capturedRequests[0].requestId(), exception);

        final var retryRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, retryRequests.length);
        assertEquals(remoteNode2, retryRequests[0].node());

        invokeHandler(retryRequests[0]);

        final var response = safeGet(future);
        assertEquals(expectedShardStateId, response.shardStateId());
    }

    public void testDoesNotRetryWhenSnapshotShardNoLongerRuns() {
        final var shardId = newShardId();
        final var snapshot = newSnapshot();
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        // Set a very short timeout so the observer fires quickly
        clusterService.getClusterSettings()
            .applySettings(
                Settings.builder()
                    .put(STATELESS_SNAPSHOT_WAIT_FOR_ACTIVE_PRIMARY_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(1))
                    .build()
            );

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, snapshot, future);

        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);

        // Fail the shard snapshot, retry stops
        setupClusterStateWithSnapshotShardState(
            remoteNode,
            ShardRoutingState.STARTED,
            snapshot,
            SnapshotsInProgress.ShardState.FAILED,
            shardId
        );
        transport.handleRemoteError(capturedRequests[0].requestId(), new ConnectTransportException(remoteNode, "connection refused"));

        final var e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("timed out waiting for primary shard"));
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testDoesNotRetryOnNonRetryableException() {
        final var shardId = newShardId();
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);

        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);

        transport.handleRemoteError(capturedRequests[0].requestId(), new ElasticsearchException("something unexpected"));

        final var e = expectThrows(ElasticsearchException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("something unexpected"));
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testDoesNotRetryWhenIndexDeletedAfterFailure() {
        final var shardId = newShardId();
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);

        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);

        // Remove the index from the cluster state before delivering the error
        setClusterStateWithEmptyProject();

        // Now deliver retryable error — but index is gone, so no retry
        transport.handleRemoteError(capturedRequests[0].requestId(), new ShardNotFoundException(shardId));

        final var e = expectThrows(Exception.class, future::actionGet);
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ShardNotFoundException.class));
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testDoesNotRetryOnLocalNodeClosing() {
        final var shardId = newShardId();
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardId);

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);

        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, capturedRequests.length);

        // Simulate local node shutting down
        transportService.stop();

        transport.handleRemoteError(capturedRequests[0].requestId(), new NodeClosedException(localNode));

        final var e = expectThrows(Exception.class, future::actionGet);
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(NodeClosedException.class));
        assertEquals(0, transport.capturedRequests().length);
    }

    public void testFailsWhenProjectOrIndexDoesNotExist() {
        final var shardId = newShardId();
        setClusterStateWithEmptyProject();

        final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
        executeAction(shardId, future);
        expectThrows(Exception.class, future::actionGet);
    }

    public void testSendIsThrottled() throws Exception {
        // Set up MAX+1 shards to verify one gets queued
        final var shardIds = new ShardId[maxInFlightSendRequests + 1];
        for (int i = 0; i < shardIds.length; i++) {
            shardIds[i] = newShardId();
            setupShardMocks(shardIds[i]);
        }
        setupClusterState(remoteNode, ShardRoutingState.STARTED, shardIds);

        final var futures = new ArrayList<PlainActionFuture<GetShardSnapshotCommitInfoResponse>>();
        for (final var shardId : shardIds) {
            final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
            executeAction(shardId, future);
            futures.add(future);
        }

        final var throttle = action.getSendThrottle();
        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(maxInFlightSendRequests, capturedRequests.length);
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(maxInFlightSendRequests));
        assertThat(AbstractThrottledTaskRunnerHelper.queuedTasks(throttle), equalTo(1));

        // Complete one request to allow the queued one to be sent
        invokeHandler(capturedRequests[0]);
        assertNotNull(safeGet(futures.getFirst()));

        var nextRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, nextRequests.length);
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(maxInFlightSendRequests));
        assertThat(AbstractThrottledTaskRunnerHelper.queuedTasks(throttle), equalTo(0));

        // Complete all remaining
        Arrays.stream(capturedRequests).skip(1).forEach(this::safeInvokeHandler);
        safeInvokeHandler(nextRequests[0]);
        futures.forEach(future -> assertNotNull(safeGet(future)));
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(0));
        assertThat(AbstractThrottledTaskRunnerHelper.queuedTasks(throttle), equalTo(0));
    }

    public void testRetryReleasesPermitAndReacquires() throws Exception {
        // Fill all permits with MAX shards on remoteNode
        final var snapshot = newSnapshot();
        final var shardIds = new ShardId[maxInFlightSendRequests];
        for (int i = 0; i < shardIds.length; i++) {
            shardIds[i] = newShardId();
            setupShardMocks(shardIds[i]);
        }
        setupClusterState(remoteNode, ShardRoutingState.STARTED, snapshot, shardIds);

        final var futures = new ArrayList<PlainActionFuture<GetShardSnapshotCommitInfoResponse>>();
        for (final var shardId : shardIds) {
            final var future = new PlainActionFuture<GetShardSnapshotCommitInfoResponse>();
            executeAction(shardId, snapshot, future);
            futures.add(future);
        }

        final var throttle = action.getSendThrottle();
        var capturedRequests = transport.getCapturedRequestsAndClear();
        assertEquals(maxInFlightSendRequests, capturedRequests.length);
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(maxInFlightSendRequests));
        assertThat(AbstractThrottledTaskRunnerHelper.queuedTasks(throttle), equalTo(0));

        // Fail the first request with a retryable error to trigger retry which acquires the permit just got released
        transport.handleRemoteError(capturedRequests[0].requestId(), new ConnectTransportException(remoteNode, "test error"));

        ClusterServiceUtils.setState(clusterService, clusterService.state());
        // All permits are still occupied: MAX-1 original + 1 retry
        var retryRequests = transport.getCapturedRequestsAndClear();
        assertEquals(1, retryRequests.length);
        assertEquals(remoteNode, retryRequests[0].node());
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(maxInFlightSendRequests));
        assertThat(AbstractThrottledTaskRunnerHelper.queuedTasks(throttle), equalTo(0));

        // Complete everything
        Arrays.stream(capturedRequests).skip(1).forEach(this::safeInvokeHandler);
        safeInvokeHandler(retryRequests[0]);
        futures.forEach(future -> assertNotNull(safeGet(future)));
        assertThat(AbstractThrottledTaskRunnerHelper.runningTasks(throttle), equalTo(0));
    }

    private void executeAction(ShardId shardId, PlainActionFuture<GetShardSnapshotCommitInfoResponse> future) {
        executeAction(shardId, new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("snap", randomUUID())), future);
    }

    private void executeAction(ShardId shardId, Snapshot snapshot, PlainActionFuture<GetShardSnapshotCommitInfoResponse> future) {
        action.execute(null, new GetShardSnapshotCommitInfoRequest(shardId, snapshot), future);
    }

    private static ShardId newShardId() {
        return new ShardId(new Index(randomIndexName(), randomUUID()), 0);
    }

    private static Snapshot newSnapshot() {
        return new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("snap", randomUUID()));
    }

    /**
     * Sets up mocks for {@code shardOperation} to execute successfully.
     * @return the expected shard state id in the response
     */
    private String setupShardMocks(ShardId shardId) throws IOException {
        final var shardStateId = randomIdentifier();
        when(snapshotsCommitService.acquireAndMaybeRegisterCommitForSnapshot(eq(shardId), any(Snapshot.class), eq(true), any())).thenReturn(
            new SnapshotsCommitService.SnapshotCommitInfo(null, Store.MetadataSnapshot.EMPTY, Map.of(), shardStateId)
        );
        return shardStateId;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void invokeHandler(CapturingTransport.CapturedRequest capturedRequest) throws Exception {
        final RequestHandlerRegistry handler = transportService.getRequestHandler(
            TransportGetShardSnapshotCommitInfoAction.SHARD_ACTION_NAME
        );
        handler.processMessageReceived(capturedRequest.request(), new TransportChannel() {
            @Override
            public String getProfileName() {
                return "default";
            }

            @Override
            public void sendResponse(TransportResponse response) {
                transport.handleResponse(capturedRequest.requestId(), response);
            }

            @Override
            public void sendResponse(Exception exception) {
                transport.handleRemoteError(capturedRequest.requestId(), exception);
            }
        });
    }

    private void safeInvokeHandler(CapturingTransport.CapturedRequest capturedRequest) {
        try {
            invokeHandler(capturedRequest);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void setupClusterState(DiscoveryNode primaryNode, ShardRoutingState routingState, ShardId... shardIds) {
        var assignments = new HashMap<ShardId, DiscoveryNode>();
        for (var shardId : shardIds) {
            assignments.put(shardId, primaryNode);
        }
        setupClusterState(routingState, assignments, null, localNode, remoteNode, remoteNode2);
    }

    private void setupClusterState(DiscoveryNode primaryNode, ShardRoutingState routingState, Snapshot snapshot, ShardId... shardIds) {
        var assignments = new HashMap<ShardId, DiscoveryNode>();
        for (var shardId : shardIds) {
            assignments.put(shardId, primaryNode);
        }
        setupClusterState(routingState, assignments, snapshot, localNode, remoteNode, remoteNode2);
    }

    private void setupClusterStateWithSnapshotShardState(
        DiscoveryNode primaryNode,
        ShardRoutingState routingState,
        Snapshot snapshot,
        SnapshotsInProgress.ShardState shardState,
        ShardId... shardIds
    ) {
        var assignments = new HashMap<ShardId, DiscoveryNode>();
        for (var shardId : shardIds) {
            assignments.put(shardId, primaryNode);
        }
        setupClusterState(routingState, assignments, snapshot, shardState, localNode, remoteNode, remoteNode2);
    }

    private void setupClusterState(Map<ShardId, DiscoveryNode> shardAssignments) {
        setupClusterState(ShardRoutingState.STARTED, shardAssignments, null, localNode, remoteNode, remoteNode2);
    }

    private void setupClusterState(Map<ShardId, DiscoveryNode> shardAssignments, DiscoveryNode... nodes) {
        setupClusterState(ShardRoutingState.STARTED, shardAssignments, null, nodes);
    }

    private void setupClusterState(
        ShardRoutingState routingState,
        Map<ShardId, DiscoveryNode> shardAssignments,
        @Nullable Snapshot snapshot,
        DiscoveryNode... nodes
    ) {
        setupClusterState(routingState, shardAssignments, snapshot, SnapshotsInProgress.ShardState.INIT, nodes);
    }

    private void setupClusterState(
        ShardRoutingState routingState,
        Map<ShardId, DiscoveryNode> shardAssignments,
        @Nullable Snapshot snapshot,
        SnapshotsInProgress.ShardState shardState,
        DiscoveryNode... nodes
    ) {
        final var projectMetadataBuilder = ProjectMetadata.builder(projectId);
        final var routingTableBuilder = RoutingTable.builder();
        shardAssignments.forEach((shardId, node) -> {
            final var indexMetadata = IndexMetadata.builder(shardId.getIndexName())
                .settings(
                    indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID()).build()
                )
                .build();
            projectMetadataBuilder.put(indexMetadata, false);
            routingTableBuilder.add(
                IndexRoutingTable.builder(shardId.getIndex())
                    .addShard(TestShardRouting.newShardRouting(shardId, node.getId(), true, routingState))
            );
        });
        final var nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId());
        Arrays.stream(nodes).forEach(nodesBuilder::add);
        final var stateBuilder = ClusterState.builder(clusterService.state())
            .nodes(nodesBuilder)
            .metadata(Metadata.builder().put(projectMetadataBuilder).build())
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTableBuilder).build());
        if (snapshot != null) {
            final var shards = new HashMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus>();
            shardAssignments.forEach((shardId, node) -> {
                shards.put(
                    shardId,
                    new SnapshotsInProgress.ShardSnapshotStatus(
                        node.getId(),
                        shardState,
                        ShardGeneration.newGeneration(random()),
                        shardState.failed() ? "test failure reason" : null
                    )
                );
            });
            final var indices = new HashMap<String, IndexId>();
            shardAssignments.keySet()
                .forEach(shardId -> indices.put(shardId.getIndexName(), new IndexId(shardId.getIndexName(), shardId.getIndex().getUUID())));
            final var entry = SnapshotsInProgress.startedEntry(
                snapshot,
                randomBoolean(),
                randomBoolean(),
                indices,
                List.of(),
                System.currentTimeMillis(),
                0L,
                shards,
                Map.of(),
                IndexVersion.current(),
                List.of()
            );
            stateBuilder.putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(entry));
        }
        ClusterServiceUtils.setState(clusterService, stateBuilder);
    }

    private void setClusterStateWithEmptyProject() {
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .nodes(DiscoveryNodes.builder().add(localNode).add(remoteNode).add(remoteNode2).localNodeId(localNode.getId()))
                .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId)).build())
                .routingTable(GlobalRoutingTable.builder().put(projectId, RoutingTable.builder()).build())
        );
    }
}
