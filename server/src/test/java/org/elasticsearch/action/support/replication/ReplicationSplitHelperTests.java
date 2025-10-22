/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationSplitHelperTests extends ESTestCase {

    public void testNeedsSplitCoordinationWithUnsetSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(2).build();

        TestReplicationRequest request = new TestReplicationRequest(new ShardId(indexName, "test-uuid", 0));
        assertFalse(ReplicationSplitHelper.needsSplitCoordination(request, indexMetadata));
    }

    public void testNeedsSplitCoordinationWithMatchingSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata)
            .reshardAddShards(2)
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(1, 2))
            .build();

        SplitShardCountSummary currentSummary = SplitShardCountSummary.forIndexing(indexMetadata, 0);
        assertThat(currentSummary, equalTo(SplitShardCountSummary.fromInt(1)));
        TestReplicationRequest request = new TestReplicationRequest(new ShardId(indexName, "test-uuid", 0), currentSummary);

        // Should return false because the split summary matches
        assertFalse(ReplicationSplitHelper.needsSplitCoordination(request, indexMetadata));
    }

    public void testNeedsSplitCoordinationWithMismatchedSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata)
            .reshardAddShards(2)
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(1, 2))
            .build();

        SplitShardCountSummary staleSummary = SplitShardCountSummary.forIndexing(indexMetadata, 0);

        indexMetadata = IndexMetadata.builder(indexMetadata)
            .reshardingMetadata(
                indexMetadata.getReshardingMetadata()
                    .transitionSplitTargetToNewState(
                        new ShardId(indexMetadata.getIndex(), 1),
                        IndexReshardingState.Split.TargetShardState.HANDOFF
                    )
            )
            .build();

        TestReplicationRequest request = new TestReplicationRequest(new ShardId(indexName, "test-uuid", 0), staleSummary);

        // Should return true because the split summary does not match
        assertTrue(ReplicationSplitHelper.needsSplitCoordination(request, indexMetadata));
    }

    @SuppressWarnings("unchecked")
    public void testNewSplitRequestAllDocumentsToCurrentPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numSourceShards = 2;

        var clusterService = mockClusterService();
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(numSourceShards).build();

        TestReplicationRequest originalRequest = new TestReplicationRequest(new ShardId(indexName, "test-uuid", 0));

        AtomicBoolean primaryExecuted = new AtomicBoolean(false);
        AtomicReference<Exception> primaryException = new AtomicReference<>();

        TestTransportReplicationAction action = new TestTransportReplicationAction(clusterService) {
            @Override
            protected Map<ShardId, TestReplicationRequest> splitRequestOnPrimary(TestReplicationRequest request) {
                // All documents go to the source shard (current primary)
                return Map.of(request.shardId(), request);
            }
        };

        // Create the split helper
        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse> splitHelper = new ReplicationSplitHelper<>(
            logger,
            clusterService,
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(1),
            null // primaryRequestSender not needed since we're not delegating
        );

        TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference shardReference =
            mock(TransportReplicationAction.PrimaryShardReference.class);

        CheckedBiConsumer<
            TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference,
            ActionListener<TestResponse>,
            Exception> executePrimaryRequest = (shardRef, listener) -> {
                primaryExecuted.set(true);
                try {
                    listener.onResponse(new TestResponse());
                } catch (Exception e) {
                    primaryException.set(e);
                    listener.onFailure(e);
                }
            };

        PlainActionFuture<TestResponse> completionListener = new PlainActionFuture<>();

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse>.SplitCoordinator coordinator = splitHelper
            .newSplitRequest(
                action,
                null, // task
                null, // project
                shardReference,
                originalRequest,
                executePrimaryRequest,
                completionListener
            );

        coordinator.coordinate();

        // Verify that the primary request was executed
        assertTrue("executePrimaryRequest should have been called", primaryExecuted.get());
        assertNull("No exception should have occurred", primaryException.get());

        // Verify the response completed successfully
        assertTrue("Completion listener should be done", completionListener.isDone());
        TestResponse response = completionListener.get();
        assertNotNull("Response should not be null", response);

        // Verify the primary shard reference was not closed since we passed it back to the action
        verify(shardReference, times(0)).close();
    }

    @SuppressWarnings("unchecked")
    public void testNewSplitRequestAllDocumentsToTarget() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final ShardId sourceShardId = new ShardId(indexName, "test-uuid", 0);
        final ShardId targetShardId = new ShardId(indexName, "test-uuid", 1);

        var clusterService = mockClusterService();
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(2).build();

        TestReplicationRequest originalRequest = new TestReplicationRequest(sourceShardId);

        // Track if executePrimaryRequest was called (it shouldn't be)
        AtomicBoolean primaryExecuted = new AtomicBoolean(false);

        // Track the sender call
        AtomicBoolean senderCalled = new AtomicBoolean(false);
        AtomicReference<ActionListener<TestResponse>> senderListener = new AtomicReference<>();

        // Create a test action that returns only the target shard
        TestTransportReplicationAction action = new TestTransportReplicationAction(clusterService) {
            @Override
            protected Map<ShardId, TestReplicationRequest> splitRequestOnPrimary(TestReplicationRequest request) {
                // All documents go to the target shard
                return Map.of(targetShardId, request);
            }
        };

        // To track phase changes
        ReplicationTask task = mock(ReplicationTask.class);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse> splitHelper = new ReplicationSplitHelper<>(
            logger,
            clusterService,
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(1),
            (node, concreteRequest, listener) -> {
                senderCalled.set(true);
                senderListener.set(listener);
            }
        );

        TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference shardReference =
            mock(TransportReplicationAction.PrimaryShardReference.class);

        CheckedBiConsumer<
            TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference,
            ActionListener<TestResponse>,
            Exception> executePrimaryRequest = (shardRef, listener) -> {
                primaryExecuted.set(true);
                listener.onResponse(new TestResponse());
            };

        PlainActionFuture<TestResponse> completionListener = new PlainActionFuture<>();

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        ProjectId projectId = mock(ProjectId.class);
        ProjectMetadata project = mock(ProjectMetadata.class);
        when(project.id()).thenReturn(projectId);
        when(project.index(targetShardId.getIndex())).thenReturn(indexMetadata);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable(projectId)).thenReturn(routingTable);
        IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardRoutingTable(targetShardId)).thenReturn(shardRoutingTable);

        AllocationId allocationId = AllocationId.newInitializing();
        RecoverySource recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        ShardRouting targetShardRouting = ShardRouting.newUnassigned(
            targetShardId,
            true,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize("node-1", allocationId.getId(), 0).moveToStarted(0);
        when(shardRoutingTable.primaryShard()).thenReturn(targetShardRouting);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        DiscoveryNode targetNode = mock(DiscoveryNode.class);
        when(discoveryNodes.get("node-1")).thenReturn(targetNode);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse>.SplitCoordinator coordinator = splitHelper
            .newSplitRequest(action, task, project, shardReference, originalRequest, executePrimaryRequest, completionListener);

        coordinator.coordinate();

        // Verify that the primary request was NOT executed (we delegated to target)
        assertFalse("executePrimaryRequest should not have been called", primaryExecuted.get());

        // Verify the primary shard reference was closed
        verify(shardReference, times(1)).close();

        // Verify task phase was set to delegation
        verify(task, times(1)).setPhase("primary_reshard_target_delegation");

        // Verify the sender was called
        assertTrue("Sender should have been called", senderCalled.get());
        assertNotNull("Sender listener should be captured", senderListener.get());

        // Complete the sender listener with success
        senderListener.get().onResponse(new TestResponse());

        // Verify task phase was set to finished
        verify(task, times(1)).setPhase("finished");

        // Verify the completion listener completed successfully
        assertTrue("Completion listener should be done", completionListener.isDone());
        TestResponse response = completionListener.get();
        assertNotNull("Response should not be null", response);
    }

    @SuppressWarnings("unchecked")
    public void testNewSplitRequestSplitBetweenPrimaryAndTarget() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final ShardId sourceShardId = new ShardId(indexName, "test-uuid", 0);
        final ShardId targetShardId = new ShardId(indexName, "test-uuid", 1);

        var clusterService = mockClusterService();
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(2).build();

        TestReplicationRequest originalRequest = new TestReplicationRequest(sourceShardId);

        AtomicBoolean primaryExecuted = new AtomicBoolean(false);
        AtomicReference<ActionListener<TestResponse>> primaryListener = new AtomicReference<>();

        AtomicBoolean senderCalled = new AtomicBoolean(false);
        AtomicReference<ActionListener<TestResponse>> senderListener = new AtomicReference<>();

        TestTransportReplicationAction action = new TestTransportReplicationAction(clusterService) {
            @Override
            protected Map<ShardId, TestReplicationRequest> splitRequestOnPrimary(TestReplicationRequest request) {
                return Map.of(
                    sourceShardId,
                    new TestReplicationRequest(sourceShardId),
                    targetShardId,
                    new TestReplicationRequest(targetShardId)
                );
            }

            @Override
            protected Tuple<TestResponse, Exception> combineSplitResponses(
                TestReplicationRequest originalRequest,
                Map<ShardId, TestReplicationRequest> splitRequests,
                Map<ShardId, Tuple<TestResponse, Exception>> responses
            ) {
                assertEquals("Should have responses from both shards", 2, responses.size());
                return new Tuple<>(new TestResponse(), null);
            }
        };

        ReplicationTask task = mock(ReplicationTask.class);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse> splitHelper = new ReplicationSplitHelper<>(
            logger,
            clusterService,
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(1),
            (node, concreteRequest, listener) -> {
                senderCalled.set(true);
                senderListener.set(listener);
            }
        );

        TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference shardReference =
            mock(TransportReplicationAction.PrimaryShardReference.class);

        CheckedBiConsumer<
            TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference,
            ActionListener<TestResponse>,
            Exception> executePrimaryRequest = (shardRef, listener) -> {
                primaryExecuted.set(true);
                primaryListener.set(listener);
            };

        PlainActionFuture<TestResponse> completionListener = new PlainActionFuture<>();

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        ProjectId projectId = mock(ProjectId.class);
        ProjectMetadata project = mock(ProjectMetadata.class);
        when(project.id()).thenReturn(projectId);
        when(project.index(targetShardId.getIndex())).thenReturn(indexMetadata);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable(projectId)).thenReturn(routingTable);
        IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardRoutingTable(targetShardId)).thenReturn(shardRoutingTable);

        AllocationId allocationId = AllocationId.newInitializing();
        RecoverySource recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        ShardRouting targetShardRouting = ShardRouting.newUnassigned(
            targetShardId,
            true,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize("node-1", allocationId.getId(), 0).moveToStarted(0);
        when(shardRoutingTable.primaryShard()).thenReturn(targetShardRouting);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        DiscoveryNode targetNode = mock(DiscoveryNode.class);
        when(discoveryNodes.get("node-1")).thenReturn(targetNode);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse>.SplitCoordinator coordinator = splitHelper
            .newSplitRequest(action, task, project, shardReference, originalRequest, executePrimaryRequest, completionListener);

        coordinator.coordinate();

        // Verify that BOTH the primary request and sender were called
        assertTrue("executePrimaryRequest should have been called", primaryExecuted.get());
        assertTrue("Sender should have been called", senderCalled.get());
        assertNotNull("Primary listener should be captured", primaryListener.get());
        assertNotNull("Sender listener should be captured", senderListener.get());

        // Verify the primary shard reference was NOT closed
        verify(shardReference, times(0)).close();

        // Verify task phase was set to the multi-request phase
        verify(task, times(1)).setPhase("primary_with_reshard_target_delegation");

        // Verify the completion listener is NOT done yet
        assertFalse("Completion listener should not be done yet", completionListener.isDone());

        if (randomBoolean()) {
            // Primary first
            primaryListener.get().onResponse(new TestResponse());
            assertFalse("Completion listener should still not be done", completionListener.isDone());
            senderListener.get().onResponse(new TestResponse());
            assertTrue("Completion listener should be done after both complete", completionListener.isDone());
        } else {
            // Target first
            senderListener.get().onResponse(new TestResponse());
            assertFalse("Completion listener should still not be done", completionListener.isDone());
            primaryListener.get().onResponse(new TestResponse());
            assertTrue("Completion listener should be done after both complete", completionListener.isDone());
        }

        // Verify task phase was set to finished
        verify(task, times(1)).setPhase("finished");

        // Verify the response completed successfully
        TestResponse response = completionListener.get();
        assertNotNull("Response should not be null", response);
    }

    /**
     * Test that split requests can complete in any order
     */
    @SuppressWarnings("unchecked")
    public void testNewSplitRequestCompletionOrder() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final ShardId sourceShardId = new ShardId(indexName, "test-uuid", 0);
        final ShardId targetShardId = new ShardId(indexName, "test-uuid", 1);

        var clusterService = mockClusterService();
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(2).build();

        TestReplicationRequest originalRequest = new TestReplicationRequest(sourceShardId);

        AtomicReference<ActionListener<TestResponse>> primaryListener = new AtomicReference<>();
        AtomicReference<ActionListener<TestResponse>> senderListener = new AtomicReference<>();

        TestTransportReplicationAction action = new TestTransportReplicationAction(clusterService) {
            @Override
            protected Map<ShardId, TestReplicationRequest> splitRequestOnPrimary(TestReplicationRequest request) {
                return Map.of(
                    sourceShardId,
                    new TestReplicationRequest(sourceShardId),
                    targetShardId,
                    new TestReplicationRequest(targetShardId)
                );
            }

            @Override
            protected Tuple<TestResponse, Exception> combineSplitResponses(
                TestReplicationRequest originalRequest,
                Map<ShardId, TestReplicationRequest> splitRequests,
                Map<ShardId, Tuple<TestResponse, Exception>> responses
            ) {
                assertEquals("Should have responses from both shards", 2, responses.size());
                return new Tuple<>(new TestResponse(), null);
            }
        };

        ReplicationTask task = mock(ReplicationTask.class);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse> splitHelper = new ReplicationSplitHelper<>(
            logger,
            clusterService,
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(1),
            (node, concreteRequest, listener) -> senderListener.set(listener)
        );

        TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference shardReference =
            mock(TransportReplicationAction.PrimaryShardReference.class);

        CheckedBiConsumer<
            TransportReplicationAction<TestReplicationRequest, TestReplicationRequest, TestResponse>.PrimaryShardReference,
            ActionListener<TestResponse>,
            Exception> executePrimaryRequest = (shardRef, listener) -> primaryListener.set(listener);

        PlainActionFuture<TestResponse> completionListener = new PlainActionFuture<>();

        // Setup cluster state mocking (same as previous test)
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        ProjectId projectId = mock(ProjectId.class);
        ProjectMetadata project = mock(ProjectMetadata.class);
        when(project.id()).thenReturn(projectId);
        when(project.index(targetShardId.getIndex())).thenReturn(indexMetadata);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(clusterState.routingTable(projectId)).thenReturn(routingTable);
        IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
        when(routingTable.shardRoutingTable(targetShardId)).thenReturn(shardRoutingTable);

        AllocationId allocationId = AllocationId.newInitializing();
        RecoverySource recoverySource = RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        ShardRouting targetShardRouting = ShardRouting.newUnassigned(
            targetShardId,
            true,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        ).initialize("node-1", allocationId.getId(), 0).moveToStarted(0);
        when(shardRoutingTable.primaryShard()).thenReturn(targetShardRouting);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        DiscoveryNode targetNode = mock(DiscoveryNode.class);
        when(discoveryNodes.get("node-1")).thenReturn(targetNode);

        ReplicationSplitHelper<TestReplicationRequest, TestReplicationRequest, TestResponse>.SplitCoordinator coordinator = splitHelper
            .newSplitRequest(action, task, project, shardReference, originalRequest, executePrimaryRequest, completionListener);

        coordinator.coordinate();

        // Complete in reverse order: target first, then primary
        senderListener.get().onResponse(new TestResponse());
        assertFalse("Completion listener should not be done after only target completes", completionListener.isDone());

        primaryListener.get().onResponse(new TestResponse());
        assertTrue("Completion listener should be done after both complete", completionListener.isDone());

        TestResponse response = completionListener.get();
        assertNotNull("Response should not be null", response);
    }

    private static ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        // Mock ThreadPool for RetryableAction
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(0L);
        when(clusterService.threadPool()).thenReturn(threadPool);

        return clusterService;
    }

    private static class TestReplicationRequest extends ReplicationRequest<TestReplicationRequest> {

        TestReplicationRequest(ShardId shardId) {
            super(shardId, SplitShardCountSummary.UNSET);
        }

        TestReplicationRequest(ShardId shardId, SplitShardCountSummary splitSummary) {
            super(shardId, splitSummary);
        }

        TestReplicationRequest(org.elasticsearch.common.io.stream.StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String toString() {
            return "TestReplicationRequest";
        }

    }

    private static class TestResponse extends ReplicationResponse {

        TestResponse() {
            setShardInfo(ReplicationResponse.ShardInfo.EMPTY);
        }

    }

    private abstract static class TestTransportReplicationAction extends TransportReplicationAction<
        TestReplicationRequest,
        TestReplicationRequest,
        TestResponse> {

        TestTransportReplicationAction(ClusterService clusterService) {
            super(
                Settings.EMPTY,
                "test-action",
                mock(TransportService.class),
                clusterService,
                mock(IndicesService.class),
                mock(ThreadPool.class),
                mock(ShardStateAction.class),
                new ActionFilters(new HashSet<>()),
                TestReplicationRequest::new,
                TestReplicationRequest::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                TransportReplicationAction.SyncGlobalCheckpointAfterOperation.DoNotSync,
                PrimaryActionExecution.RejectOnOverload,
                TransportReplicationAction.ReplicaActionExecution.SubjectToCircuitBreaker
            );
        }

        protected abstract Map<ShardId, TestReplicationRequest> splitRequestOnPrimary(TestReplicationRequest request);

        @Override
        protected Tuple<TestResponse, Exception> combineSplitResponses(
            TestReplicationRequest originalRequest,
            Map<ShardId, TestReplicationRequest> splitRequests,
            Map<ShardId, Tuple<TestResponse, Exception>> responses
        ) {
            assert responses.size() == 1;
            return responses.values().iterator().next();
        }

        @Override
        protected TestResponse newResponseInstance(org.elasticsearch.common.io.stream.StreamInput in) throws IOException {
            return new TestResponse();
        }

        @Override
        protected void shardOperationOnPrimary(
            TestReplicationRequest shardRequest,
            IndexShard primary,
            ActionListener<PrimaryResult<TestReplicationRequest, TestResponse>> listener
        ) {
            listener.onResponse(new PrimaryResult<>(shardRequest, new TestResponse()));
        }

        @Override
        protected void shardOperationOnReplica(TestReplicationRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
            listener.onResponse(new ReplicaResult());
        }
    }
}
