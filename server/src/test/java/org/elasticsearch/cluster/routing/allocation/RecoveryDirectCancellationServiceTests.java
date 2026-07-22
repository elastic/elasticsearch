/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.ShardRecoveryCancellation;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestLogging(
    reason = "asserting direct cancellation logs",
    value = "org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService:DEBUG"
)
public class RecoveryDirectCancellationServiceTests extends ESAllocationTestCase {

    public void testComputeDirectCancellationCandidates() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        final var index = indexMetadata.getIndex();
        final var undesiredShardId = new ShardId(index, 0);
        final var desiredShardId = new ShardId(index, 1);
        final var undesiredReplicaAllocationId = AllocationId.newInitializing(randomIdentifier("undesired-"));
        final var desiredReplicaAllocationId = AllocationId.newInitializing(randomIdentifier("desired-"));

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(undesiredShardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(undesiredShardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(undesiredReplicaAllocationId)
                    .build()
            )
            .addShard(newShardRouting(desiredShardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(desiredShardId, "node-2", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(desiredReplicaAllocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        final var balance = new DesiredBalance(
            1,
            Map.of(
                undesiredShardId,
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                desiredShardId,
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)
            )
        );

        final var routingAllocation = createRoutingAllocationFrom(clusterState);
        final var requests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, routingAllocation);

        assertThat(requests.entrySet(), hasSize(1));
        final var node1 = clusterState.nodes().get("node-1");
        final var request = requests.get(node1);
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(undesiredShardId));
        assertThat(cancellation.allocationId(), equalTo(undesiredReplicaAllocationId.getId()));
        assertFalse(cancellation.cancelIfStarted());

        final var forbidRemainOnNode1 = forbidRemainDecider(undesiredShardId, "node-1", false);
        final var routingAllocationWithForbidRemain = createRoutingAllocationFrom(clusterState, forbidRemainOnNode1);
        final var escalatedRequests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(
            balance,
            routingAllocationWithForbidRemain
        );

        assertThat(escalatedRequests.entrySet(), hasSize(1));
        assertTrue(escalatedRequests.get(node1).cancellations().getFirst().cancelIfStarted());
    }

    public void testDirectCancellationCandidatesForInitializingPrimary() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var primaryAllocationId = AllocationId.newInitializing(randomIdentifier("primary-"));

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-1", true, ShardRoutingState.INITIALIZING)
                    .withAllocationId(primaryAllocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-2"), 1, 0, 0)));
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", true);

        final var requests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(
            balance,
            createRoutingAllocationFrom(clusterState, forbidRemain)
        );

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(shardId));
        assertThat(cancellation.allocationId(), equalTo(primaryAllocationId.getId()));
        assertFalse(cancellation.cancelIfStarted());
    }

    public void testDirectCancellationCandidatesForPrimaryRelocation() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(
                RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(newShardRouting(shardId, "node-0", true, STARTED)))
            )
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-2"), 1, 0, 0)));
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", true);

        final var allocation = createRoutingAllocationFrom(clusterState, forbidRemain);
        final var startedPrimary = allocation.routingNodes().node("node-0").getByShardId(shardId);
        allocation.routingNodes()
            .relocateShard(startedPrimary, "node-1", ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, "test-setup", allocation.changes());

        final var requests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, allocation);

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(shardId));
        assertTrue(cancellation.cancelIfStarted());
    }

    public void testDirectCancellationCandidatesDoNotEscalateSoleSearchableCopy() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var searchOnlyAllocationId = AllocationId.newInitializing(randomIdentifier("search-only-replica-"));

        final var indexRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addShard(newShardRouting(shardId, "node-0", true, STARTED))
                    .addShard(
                        TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                            .withAllocationId(searchOnlyAllocationId)
                            .withRole(ShardRouting.Role.SEARCH_ONLY)
                            .build()
                    )
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(indexRoutingTable)
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", true);

        final var allocation = createRoutingAllocationFrom(clusterState, forbidRemain);
        final var requests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, allocation);

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(shardId));
        assertThat(cancellation.allocationId(), equalTo(searchOnlyAllocationId.getId()));
        assertFalse(cancellation.cancelIfStarted());
    }

    public void testDirectCancellationCandidatesEscalateSearchableCopyWhenAnotherStartedCopyExists() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var initializingSearchOnlyAllocationId = AllocationId.newInitializing(randomIdentifier("initializing-search-only-"));

        final var indexRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addShard(newShardRouting(shardId, "node-0", true, STARTED))
                    .addShard(
                        TestShardRouting.shardRoutingBuilder(shardId, "node-2", false, STARTED)
                            .withRole(ShardRouting.Role.SEARCH_ONLY)
                            .build()
                    )
                    .addShard(
                        TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                            .withAllocationId(initializingSearchOnlyAllocationId)
                            .withRole(ShardRouting.Role.SEARCH_ONLY)
                            .build()
                    )
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(indexRoutingTable)
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", false);

        final var allocation = createRoutingAllocationFrom(clusterState, forbidRemain);
        final var requests = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, allocation);

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(shardId));
        assertThat(cancellation.allocationId(), equalTo(initializingSearchOnlyAllocationId.getId()));
        assertTrue(cancellation.cancelIfStarted());
    }

    public void testDisabledDirectCancellationsAreLoggedAndNotSent() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var replicaAllocationId = AllocationId.newInitializing(randomIdentifier("replica-"));
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(shardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(replicaAllocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));
        final var routingAllocation = createRoutingAllocationFrom(clusterState, forbidRemainDecider(shardId, "node-1", false));
        final var expectedCancellation = new ShardRecoveryCancellation(shardId, replicaAllocationId.getId(), true);

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sendRequestCalled = new AtomicBoolean();
        doAnswer(ignored -> {
            sendRequestCalled.set(true);
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var state = mock(ClusterState.class);
        when(state.getMinTransportVersion()).thenReturn(TransportVersion.current());
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(state, false),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );

        try (var mockLog = MockLog.capture(RecoveryDirectCancellationService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "disabled direct cancellation log",
                    RecoveryDirectCancellationService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*is disabled, would have sent direct recovery cancellations*" + expectedCancellation.allocationId() + "*"
                )
            );
            service.computeAndSubmitCancellations(desiredBalance, routingAllocation);
            taskQueue.runAllRunnableTasks();
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse(sendRequestCalled.get());
    }

    public void testUnsupportedTransportVersionDirectCancellationsAreLoggedAndNotSent() {
        final var unsupportedTransportVersion = TransportVersionUtils.getPreviousVersion(
            CancelRecoveriesAction.DIRECT_RECOVERY_CANCELLATION,
            true
        );
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var replicaAllocationId = AllocationId.newInitializing(randomIdentifier("replica-"));
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(shardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(replicaAllocationId)
                    .build()
            );
        final var replicaClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));
        final var routingAllocation = createRoutingAllocationFrom(replicaClusterState, forbidRemainDecider(shardId, "node-1", false));
        final var expectedCancellation = new ShardRecoveryCancellation(shardId, replicaAllocationId.getId(), true);
        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sendRequestCalled = new AtomicBoolean();
        doAnswer(invocation -> {
            sendRequestCalled.set(true);
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());
        final var clusterState = mock(ClusterState.class);
        when(clusterState.getMinTransportVersion()).thenReturn(unsupportedTransportVersion);
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(clusterState, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );

        try (var mockLog = MockLog.capture(RecoveryDirectCancellationService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unsupported transport version log",
                    RecoveryDirectCancellationService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*not every node in the cluster supports direct recovery cancellation yet*" + expectedCancellation.allocationId() + "*"
                )
            );
            service.computeAndSubmitCancellations(desiredBalance, routingAllocation);
            taskQueue.runAllRunnableTasks();
            mockLog.assertAllExpectationsMatched();
        }

        assertFalse(sendRequestCalled.get());
    }

    public void testCachedRequestsAreDeduplicated() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 3, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId0 = new ShardId(index, 0);
        final var shardId1 = new ShardId(index, 1);
        final var shardId2 = new ShardId(index, 2);
        final var allocationId0 = AllocationId.newInitializing(randomIdentifier("alloc-0-"));
        final var allocationId1 = AllocationId.newInitializing(randomIdentifier("alloc-1-"));
        final var allocationId2 = AllocationId.newInitializing(randomIdentifier("alloc-2-"));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sentRequests = new CopyOnWriteArrayList<CancelRecoveriesAction.Request>();

        final var state = mock(ClusterState.class);
        when(state.getMinTransportVersion()).thenReturn(TransportVersion.current());
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(state, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );

        doAnswer(invocation -> {
            CancelRecoveriesAction.Request req = invocation.getArgument(2);
            TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            sentRequests.add(req);
            handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        // First round: send cancellations for shard 0 and shard 1
        {
            final var indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(newShardRouting(shardId0, "node-0", true, STARTED))
                .addShard(
                    TestShardRouting.shardRoutingBuilder(shardId0, "node-1", false, ShardRoutingState.INITIALIZING)
                        .withAllocationId(allocationId0)
                        .build()
                )
                .addShard(newShardRouting(shardId1, "node-0", true, STARTED))
                .addShard(
                    TestShardRouting.shardRoutingBuilder(shardId1, "node-1", false, ShardRoutingState.INITIALIZING)
                        .withAllocationId(allocationId1)
                        .build()
                )
                .addShard(newShardRouting(shardId2, "node-0", true, STARTED))
                .addShard(newShardRouting(shardId2, "node-2", false, STARTED));
            final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(discoveryNodes(3))
                .metadata(Metadata.builder().put(indexMetadata, true))
                .routingTable(RoutingTable.builder().add(indexRoutingTable))
                .build();
            final var balance = new DesiredBalance(
                1,
                Map.of(
                    shardId0,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                    shardId1,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                    shardId2,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)
                )
            );
            service.computeAndSubmitCancellations(balance, createRoutingAllocationFrom(clusterState));
            taskQueue.runAllRunnableTasks();

            assertThat(sentRequests, hasSize(1));
            final var sentCancellations = sentRequests.getFirst().cancellations();
            assertThat(sentCancellations, hasSize(2));
            assertThat(
                sentCancellations,
                containsInAnyOrder(
                    new ShardRecoveryCancellation(shardId0, allocationId0.getId(), false),
                    new ShardRecoveryCancellation(shardId1, allocationId1.getId(), false)
                )
            );
        }

        // Second round: same cancellations for shard 0 and shard 1, new cancellation for shard 2. Only shard 2 should be sent.
        sentRequests.clear();
        {
            final var indexRoutingTable = IndexRoutingTable.builder(index)
                .addShard(newShardRouting(shardId0, "node-0", true, STARTED))
                .addShard(
                    TestShardRouting.shardRoutingBuilder(shardId0, "node-1", false, ShardRoutingState.INITIALIZING)
                        .withAllocationId(allocationId0)
                        .build()
                )
                .addShard(newShardRouting(shardId1, "node-0", true, STARTED))
                .addShard(
                    TestShardRouting.shardRoutingBuilder(shardId1, "node-1", false, ShardRoutingState.INITIALIZING)
                        .withAllocationId(allocationId1)
                        .build()
                )
                .addShard(newShardRouting(shardId2, "node-0", true, STARTED))
                .addShard(
                    TestShardRouting.shardRoutingBuilder(shardId2, "node-1", false, ShardRoutingState.INITIALIZING)
                        .withAllocationId(allocationId2)
                        .build()
                );
            final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(discoveryNodes(3))
                .metadata(Metadata.builder().put(indexMetadata, true))
                .routingTable(RoutingTable.builder().add(indexRoutingTable))
                .build();
            final var balance = new DesiredBalance(
                2,
                Map.of(
                    shardId0,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                    shardId1,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                    shardId2,
                    new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)
                )
            );
            service.computeAndSubmitCancellations(balance, createRoutingAllocationFrom(clusterState));
            taskQueue.runAllRunnableTasks();

            assertThat(sentRequests, hasSize(1));
            final var sentCancellations = sentRequests.getFirst().cancellations();
            assertThat(sentCancellations, hasSize(1));
            assertThat(sentCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId2, allocationId2.getId(), false)));
        }

        // Verify cache contains all three allocation IDs
        assertThat(service.sentCancellations.get(allocationId0.getId()), notNullValue());
        assertThat(service.sentCancellations.get(allocationId1.getId()), notNullValue());
        assertThat(service.sentCancellations.get(allocationId2.getId()), notNullValue());
    }

    public void testFailedRequestsAreInvalidated() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var allocationId = AllocationId.newInitializing(randomIdentifier("alloc-"));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sentRequests = new CopyOnWriteArrayList<CancelRecoveriesAction.Request>();

        final var state = mock(ClusterState.class);
        when(state.getMinTransportVersion()).thenReturn(TransportVersion.current());
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(state, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );

        doAnswer(invocation -> {
            CancelRecoveriesAction.Request req = invocation.getArgument(2);
            TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            sentRequests.add(req);
            handler.handleException(new ConnectTransportException(invocation.getArgument(0), "oops"));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(shardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(allocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));

        service.computeAndSubmitCancellations(balance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var firstRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(firstRoundCancellations, hasSize(1));
        assertThat(firstRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        sentRequests.clear();
        service.computeAndSubmitCancellations(balance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var secondRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(secondRoundCancellations, hasSize(1));
        assertThat(secondRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        // Verify cache does not contain failed request
        assertThat(service.sentCancellations.get(allocationId.getId()), nullValue());
    }

    public void testRequestWithDifferentCancelIfStartedDoesNotGetDeduplicated() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var allocationId = AllocationId.newInitializing(randomIdentifier("alloc-"));
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(shardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(allocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sentRequests = new CopyOnWriteArrayList<CancelRecoveriesAction.Request>();

        doAnswer(invocation -> {
            CancelRecoveriesAction.Request req = invocation.getArgument(2);
            TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            sentRequests.add(req);
            handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var state = mock(ClusterState.class);
        when(state.getMinTransportVersion()).thenReturn(TransportVersion.current());
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(state, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );

        // First round: cancelIfStarted=false
        service.computeAndSubmitCancellations(desiredBalance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var firstRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(firstRoundCancellations, hasSize(1));
        assertThat(firstRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        // Second round: cancelIfStarted=true
        sentRequests.clear();
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", false);
        service.computeAndSubmitCancellations(desiredBalance, createRoutingAllocationFrom(clusterState, forbidRemain));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var secondRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(secondRoundCancellations, hasSize(1));
        assertThat(secondRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), true)));

        // Third round: cancelIfStarted=true again
        sentRequests.clear();
        service.computeAndSubmitCancellations(desiredBalance, createRoutingAllocationFrom(clusterState, forbidRemain));
        taskQueue.runAllRunnableTasks();
        assertThat(sentRequests, hasSize(0));
    }

    private static AllocationDecider forbidRemainDecider(ShardId shardId, String forbiddenNodeId, boolean primary) {
        return new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return shardRouting.shardId().equals(shardId) && shardRouting.primary() == primary && node.nodeId().equals(forbiddenNodeId)
                    ? Decision.NO
                    : Decision.YES;
            }
        };
    }

    private static RoutingAllocation createRoutingAllocationFrom(ClusterState clusterState, AllocationDecider... deciders) {
        return TestRoutingAllocationFactory.forClusterState(clusterState).allocationDeciders(deciders).mutable();
    }

    private ClusterService createMockClusterService(ClusterState clusterState, boolean enableDirectCancellations) {
        final Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING);
        final var initialSettings = Settings.builder()
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), enableDirectCancellations)
            .build();
        final var clusterSettings = new ClusterSettings(initialSettings, settingSet);

        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
        doReturn(mock(MasterServiceTaskQueue.class)).when(clusterService).createTaskQueue(anyString(), any(Priority.class), any());
        return clusterService;
    }

    private static DiscoveryNodes discoveryNodes(int nodeCount) {
        final var discoveryNodes = DiscoveryNodes.builder();
        for (var i = 0; i < nodeCount; i++) {
            discoveryNodes.add(newNode("node-" + i, "node-" + i, Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)));
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");
        return discoveryNodes.build();
    }
}
