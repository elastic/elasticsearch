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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
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
import org.elasticsearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.CancelRecoveriesAction;
import org.elasticsearch.indices.recovery.ShardRecoveryCancellation;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.rarely;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoveryDirectCancellationServiceTests extends ESAllocationTestCase {

    public void testComputeUndesiredRecoveryCancellations() {
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
        final var requests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(balance, routingAllocation);

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
        final var escalatedRequests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(
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

        final var requests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(
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
            .relocateShard(
                startedPrimary,
                "node-1",
                ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE,
                "test-setup",
                allocation.changes(),
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            );

        final var requests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(balance, allocation);

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
        final var requests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(balance, allocation);

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
        final var requests = RecoveryDirectCancellationService.computeUndesiredRecoveryCancellations(balance, allocation);

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(shardId));
        assertThat(cancellation.allocationId(), equalTo(initializingSearchOnlyAllocationId.getId()));
        assertTrue(cancellation.cancelIfStarted());
    }

    @TestLogging(
        reason = "asserting direct cancellation logs",
        value = "org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService:DEBUG"
    )
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
        service.start();

        try (var mockLog = MockLog.capture(RecoveryDirectCancellationService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "disabled direct cancellation log",
                    RecoveryDirectCancellationService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*is disabled, would have sent direct recovery cancellations*" + expectedCancellation.allocationId() + "*"
                )
            );
            service.cancelUndesiredRecoveries(desiredBalance, routingAllocation);
            taskQueue.runAllRunnableTasks();
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse(sendRequestCalled.get());
    }

    @TestLogging(
        reason = "asserting direct cancellation logs",
        value = "org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService:DEBUG"
    )
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
        service.start();
        try (var mockLog = MockLog.capture(RecoveryDirectCancellationService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "unsupported transport version log",
                    RecoveryDirectCancellationService.class.getCanonicalName(),
                    Level.DEBUG,
                    "*not every node in the cluster supports direct recovery cancellation yet*" + expectedCancellation.allocationId() + "*"
                )
            );
            service.cancelUndesiredRecoveries(desiredBalance, routingAllocation);
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
        service.start();
        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
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
            service.cancelUndesiredRecoveries(balance, createRoutingAllocationFrom(clusterState));
            taskQueue.runAllRunnableTasks();

            assertThat(sentRequests, hasSize(1));
            final var cancellations = sentRequests.getFirst().cancellations();
            assertThat(cancellations, hasSize(2));
            assertThat(
                cancellations,
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
            service.cancelUndesiredRecoveries(balance, createRoutingAllocationFrom(clusterState));
            taskQueue.runAllRunnableTasks();

            assertThat(sentRequests, hasSize(1));
            final var cancellations = sentRequests.getFirst().cancellations();
            assertThat(cancellations, hasSize(1));
            assertThat(cancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId2, allocationId2.getId(), false)));
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
        service.start();
        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
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

        service.cancelUndesiredRecoveries(balance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var firstRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(firstRoundCancellations, hasSize(1));
        assertThat(firstRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        sentRequests.clear();
        service.cancelUndesiredRecoveries(balance, createRoutingAllocationFrom(clusterState));
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
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
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
        service.start();
        // First round: cancelIfStarted=false
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var firstRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(firstRoundCancellations, hasSize(1));
        assertThat(firstRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        // Second round: cancelIfStarted=true
        sentRequests.clear();
        final var forbidRemain = forbidRemainDecider(shardId, "node-1", false);
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterState, forbidRemain));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var secondRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(secondRoundCancellations, hasSize(1));
        assertThat(secondRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), true)));

        // Third round: cancelIfStarted=true again
        sentRequests.clear();
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterState, forbidRemain));
        taskQueue.runAllRunnableTasks();
        assertThat(sentRequests, hasSize(0));
    }

    public void testRequestWithChangedTermDoesNotGetDeduplicated() {
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
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sentRequests = new CopyOnWriteArrayList<CancelRecoveriesAction.Request>();

        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
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
        service.start();
        final var clusterStateWithTermOne = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true).coordinationMetadata(CoordinationMetadata.builder().term(1L).build()))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        // First round with term=1, cancellation is sent
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermOne));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var firstRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(firstRoundCancellations, hasSize(1));
        assertThat(firstRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));

        // Second round with term=1, cancellation is deduplicated
        sentRequests.clear();
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermOne));
        taskQueue.runAllRunnableTasks();
        assertThat(sentRequests, hasSize(0));

        // Third round with term=2, term changed, so the cached entry is bypassed and the cancellation is re-sent
        sentRequests.clear();
        final var clusterStateWithTermTwo = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true).coordinationMetadata(CoordinationMetadata.builder().term(2L).build()))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermTwo));
        taskQueue.runAllRunnableTasks();

        assertThat(sentRequests, hasSize(1));
        final var thirdRoundCancellations = sentRequests.getFirst().cancellations();
        assertThat(thirdRoundCancellations, hasSize(1));
        assertThat(thirdRoundCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, allocationId.getId(), false)));
    }

    public void testStaleFailureHandlerDoesNotInvalidateNewerCacheEntry() {
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
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var sentRequests = new CopyOnWriteArrayList<CancelRecoveriesAction.Request>();

        final var capturedHandler = new AtomicReference<TransportResponseHandler<CancelRecoveriesAction.Response>>();
        final var requestsCaptured = new AtomicInteger(0);

        doAnswer(invocation -> {
            sentRequests.add(invocation.getArgument(2));
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            if (requestsCaptured.incrementAndGet() == 1) {
                // First request, hold off on calling the handler to simulate an in-flight request, then fail
                capturedHandler.set(handler);
            } else {
                handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            }
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
        service.start();
        final var clusterStateWithTermOne = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true).coordinationMetadata(CoordinationMetadata.builder().term(1L).build()))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
        final var clusterStateWithTermTwo = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true).coordinationMetadata(CoordinationMetadata.builder().term(2L).build()))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        // Request sent but held in-flight
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermOne));
        taskQueue.runAllRunnableTasks();

        // Term changed, bypass fires, new request sent and acknowledged
        sentRequests.clear();
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermTwo));
        taskQueue.runAllRunnableTasks();
        assertThat(sentRequests, hasSize(1));

        // The in-flight term=1 request now fails
        capturedHandler.get().handleException(new ConnectTransportException(mock(DiscoveryNode.class), "oops"));
        taskQueue.runAllRunnableTasks();
        assertThat(service.sentCancellations.get(allocationId.getId()), notNullValue());

        // Next request is deduplicated
        sentRequests.clear();
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterStateWithTermTwo));
        taskQueue.runAllRunnableTasks();
        assertThat(sentRequests, hasSize(0));
    }

    /// Randomized test that simulates bounded-cache evictions, failed requests, new requests, and cancelIfStarted/term
    /// bumps, interleaved in random order. Verifies that after every round the service has sent all and only the
    /// expected requests.
    public void testCacheInvalidationAndCancellationsInterleaving() {
        final int numShards = randomIntBetween(2, 10);
        final int numRounds = randomIntBetween(5, 10);

        final var indexMetadata = IndexMetadata.builder(randomIndexName())
            .settings(indexSettings(IndexVersion.current(), numShards, 1))
            .build();
        final var index = indexMetadata.getIndex();

        // Keep it simple, one shard/allocationId per node
        final var allocationIds = new HashMap<String, String>(numShards);
        final var indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        final var discoveryNodesBuilder = DiscoveryNodes.builder();
        discoveryNodesBuilder.add(newNode("master-node", "master-node", Set.of(DiscoveryNodeRole.MASTER_ROLE)));

        final var shardAssignments = new HashMap<ShardId, ShardAssignment>();
        for (int i = 0; i < numShards; i++) {
            final var shardId = new ShardId(index, i);
            final var primaryNodeId = "primary-node-" + i;
            final var dataNodeId = "data-node-" + i;
            final var allocId = AllocationId.newInitializing(randomIdentifier("alloc-" + i + "-"));
            allocationIds.put(dataNodeId, allocId.getId());
            discoveryNodesBuilder.add(newNode(primaryNodeId, primaryNodeId, Set.of(DiscoveryNodeRole.DATA_ROLE)));
            discoveryNodesBuilder.add(newNode(dataNodeId, dataNodeId, Set.of(DiscoveryNodeRole.DATA_ROLE)));
            indexRoutingTableBuilder.addShard(newShardRouting(shardId, primaryNodeId, true, STARTED));
            indexRoutingTableBuilder.addShard(
                TestShardRouting.shardRoutingBuilder(shardId, dataNodeId, false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(allocId)
                    .build()
            );
            // Primary can stay on its node, replicas should all move to "desired-node"
            shardAssignments.put(shardId, new ShardAssignment(Set.of(primaryNodeId, "desired-node"), 2, 0, 0));
        }
        discoveryNodesBuilder.masterNodeId("master-node").localNodeId("master-node");

        final var discoveryNodes = discoveryNodesBuilder.build();
        final var routingTable = RoutingTable.builder().add(indexRoutingTableBuilder).build();
        final var balance = new DesiredBalance(1, shardAssignments);

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());

        // IDs which are expected to be in the cache at the end of each round (value = cancelIfStarted)
        final Map<String, Boolean> expectedCached = new HashMap<>();
        // IDs whose transport request will fail in the current round
        final Set<String> failThisRound = new HashSet<>();
        // Sent IDs captured in the current round
        final Set<String> actualSentThisRound = new HashSet<>();

        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            // Each data node has one initializing replica
            assertThat(req.cancellations(), hasSize(1));
            final String allocId = req.cancellations().getFirst().allocationId();
            final String nodeId = ((DiscoveryNode) invocation.getArgument(0)).getId();
            assertThat("cancellation sent to unexpected node " + nodeId, allocId, equalTo(allocationIds.get(nodeId)));
            actualSentThisRound.add(allocId);
            if (failThisRound.contains(allocId)) {
                handler.handleException(new ConnectTransportException(invocation.getArgument(0), "simulated transport failure"));
            } else {
                handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            }
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var serviceClusterState = mock(ClusterState.class);
        when(serviceClusterState.getMinTransportVersion()).thenReturn(TransportVersion.current());
        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(serviceClusterState, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );
        service.start();
        long currentTerm = 0;
        for (int round = 0; round < numRounds; round++) {
            // Invalidate some entries to simulate the cache reaching its size or TTL bound. Technically, invalidation
            // uses a different path than eviction but they both end up calling [CacheSegment#remove] and [Cache#delete].
            final var it = expectedCached.entrySet().iterator();
            while (it.hasNext()) {
                final var cached = it.next();
                if (randomBoolean()) {
                    service.sentCancellations.invalidate(cached.getKey());
                    it.remove();
                }
            }

            final var expectedSentThisRound = new HashSet<String>();
            // If we bump the master term, every request is expected to be re-sent
            if (rarely()) {
                currentTerm++;
                for (final String allocId : allocationIds.values()) {
                    expectedCached.put(allocId, randomBoolean());
                    expectedSentThisRound.add(allocId);
                }
            } else {
                for (final String allocId : allocationIds.values()) {
                    if (expectedCached.containsKey(allocId) == false) {
                        expectedSentThisRound.add(allocId);
                        expectedCached.put(allocId, randomBoolean());
                    } else {
                        // Randomly promote some allocationIds to cancelIfStarted=true
                        if (randomBoolean() && expectedCached.get(allocId) == false) {
                            expectedSentThisRound.add(allocId);
                            expectedCached.put(allocId, true);
                        }
                    }
                }
            }

            final var cancelIfStartedThisRound = expectedCached.keySet()
                .stream()
                .filter(expectedCached::get)
                .collect(Collectors.toUnmodifiableSet());

            // Build a decider that makes canRemain return NO for shards who have cancelIfStarted=true
            final var cancelIfStartedDecider = new AllocationDecider() {
                @Override
                public Decision canRemain(
                    IndexMetadata indexMetadata,
                    ShardRouting shardRouting,
                    RoutingNode node,
                    RoutingAllocation allocation
                ) {
                    final String allocId = allocationIds.get(node.nodeId());
                    return cancelIfStartedThisRound.contains(allocId) ? Decision.NO : randomFrom(Decision.NOT_PREFERRED, Decision.YES);
                }
            };

            // Randomly pick which of the expected sends will fail
            failThisRound.clear();
            for (final String allocId : expectedSentThisRound) {
                if (randomBoolean() && randomBoolean()) {
                    failThisRound.add(allocId);
                    expectedCached.remove(allocId);
                }
            }

            final var clusterStateThisRound = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(discoveryNodes)
                .metadata(
                    Metadata.builder()
                        .put(indexMetadata, true)
                        .coordinationMetadata(CoordinationMetadata.builder().term(currentTerm).build())
                        .build()
                )
                .routingTable(routingTable)
                .build();

            actualSentThisRound.clear();
            service.cancelUndesiredRecoveries(balance, createRoutingAllocationFrom(clusterStateThisRound, cancelIfStartedDecider));
            taskQueue.runAllRunnableTasks();

            assertThat("round " + round + ": sent expected allocation IDs", actualSentThisRound, equalTo(expectedSentThisRound));

            // Verify the service cache matches our tracking
            for (final String allocId : allocationIds.values()) {
                if (expectedCached.containsKey(allocId)) {
                    assertThat(
                        "allocation ID " + allocId + " should be in cache",
                        service.sentCancellations.get(allocId),
                        equalTo(new RecoveryDirectCancellationService.SentCancellation(currentTerm, expectedCached.get(allocId)))
                    );
                } else {
                    assertThat("allocation ID " + allocId + " should not be in cache", service.sentCancellations.get(allocId), nullValue());
                }
            }
        }
    }

    public void testCancellationForWaitingSnapshot() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        final var snapshot = snapshotWithShards(
            Map.of(waitingShardId, new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null))
        );
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            );
        final var clusterState = clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot);

        final var requests = RecoveryDirectCancellationService.computeCancellationCandidatesForSnapshots(clusterState);
        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        assertThat(
            request.cancellations().getFirst(),
            equalTo(new ShardRecoveryCancellation(waitingShardId, targetAllocationId.getId(), false))
        );
    }

    public void testCancelsRecoveryBlockingSnapshotWhenRelocatingShardTarget() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 3, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var nonWaitingShardId = new ShardId(index, 1);
        final var nonInitializingShardId = new ShardId(index, 2);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        final var snapshot = snapshotWithShards(
            Map.of(
                waitingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                nonWaitingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.INIT, null),
                nonInitializingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null)
            )
        );
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(
                TestShardRouting.shardRoutingBuilder(nonWaitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("non-waiting-"))))
                    .withRelocatingNodeId("node-2")
                    .build()
            )
            .addShard(newShardRouting(nonInitializingShardId, "node-2", true, STARTED));
        final var clusterState = clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot);

        final var requests = RecoveryDirectCancellationService.computeCancellationCandidatesForSnapshots(clusterState);

        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        final var cancellation = request.cancellations().getFirst();
        assertThat(cancellation, equalTo(new ShardRecoveryCancellation(waitingShardId, targetAllocationId.getId(), false)));
    }

    public void testDoesNotCancelRecoveryBlockingSnapshotWhenNonRelocating() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var nonRelocatingShardId = new ShardId(index, 0);
        final var startedShardId = new ShardId(index, 1);

        final var snapshot = snapshotWithShards(
            Map.of(
                nonRelocatingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-1", SnapshotsInProgress.ShardState.WAITING, null),
                startedShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(TestShardRouting.shardRoutingBuilder(nonRelocatingShardId, "node-1", true, ShardRoutingState.INITIALIZING).build())
            .addShard(TestShardRouting.newShardRouting(startedShardId, "node-2", true, STARTED));
        final var clusterState = clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot);

        final var requests = RecoveryDirectCancellationService.computeCancellationCandidatesForSnapshots(clusterState);
        assertThat(requests.entrySet(), hasSize(0));
    }

    public void testDoesNotCancelRecoveryBlockingSnapshotWhenSourceNodeMarkedForRemoval() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shutdownBlockedShardId = new ShardId(index, 0);
        final var startedShardId = new ShardId(index, 1);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));

        final var snapshot = snapshotWithShards(
            Map.of(
                shutdownBlockedShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                startedShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(shutdownBlockedShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(TestShardRouting.newShardRouting(startedShardId, "node-2", true, STARTED));
        final var removalType = randomFrom(
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.REPLACE,
            SingleNodeShutdownMetadata.Type.SIGTERM
        );
        final var shutdownMetadata = new NodesShutdownMetadata(Map.of("node-0", nodeShutdownMetadata("node-0", removalType)));
        final var clusterState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .metadata(
                Metadata.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot).metadata())
                    .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata)
            )
            .build();

        final var requests = RecoveryDirectCancellationService.computeCancellationCandidatesForSnapshots(clusterState);
        assertThat(requests.entrySet(), hasSize(0));
    }

    public void testStillCancelsRecoveryBlockingSnapshotWhenSourceNodeRestarting() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var startedShardId = new ShardId(index, 1);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        final var snapshot = snapshotWithShards(
            Map.of(
                waitingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                startedShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(TestShardRouting.newShardRouting(startedShardId, "node-2", true, STARTED));
        final var shutdownMetadata = new NodesShutdownMetadata(
            Map.of("node-0", nodeShutdownMetadata("node-0", SingleNodeShutdownMetadata.Type.RESTART))
        );
        final var clusterState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .metadata(
                Metadata.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot).metadata())
                    .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata)
            )
            .build();

        final var requests = RecoveryDirectCancellationService.computeCancellationCandidatesForSnapshots(clusterState);
        assertThat(requests.entrySet(), hasSize(1));
        final var request = requests.get(clusterState.nodes().get("node-1"));
        assertNotNull(request);
        assertThat(request.cancellations(), hasSize(1));
        assertThat(
            request.cancellations().getFirst(),
            equalTo(new ShardRecoveryCancellation(waitingShardId, targetAllocationId.getId(), false))
        );
    }

    public void testSnapshotAndDesiredBalanceCancellationsCacheSharing() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var initShardId = new ShardId(index, 1);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        // Primary is relocating from node-0 to node-1. Both the desired-balance path and the snapshot path
        // target the INITIALIZING relocation target's allocationId.
        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(shardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(newShardRouting(initShardId, "node-2", true, STARTED));
        final var snapshot = snapshotWithShards(
            Map.of(
                shardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                initShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var compatVersions = new CompatibilityVersions(TransportVersion.current(), Map.of());
        final var clusterState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .putCompatibilityVersions("node-0", compatVersions)
            .putCompatibilityVersions("node-1", compatVersions)
            .putCompatibilityVersions("node-2", compatVersions)
            .build();

        // Desired balance says shard should be on node-2, not node-1
        final var desiredBalance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-2"), 1, 0, 0)));

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());

        final var capturedCancellations = new CopyOnWriteArrayList<ShardRecoveryCancellation>();
        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            capturedCancellations.addAll(req.cancellations());
            handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(clusterState, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );
        service.start();

        // Snapshot path via clusterChanged sends cancelIfStarted=false for the relocating shard
        final var previousState = ClusterState.builder(clusterState).removeCustom(SnapshotsInProgress.TYPE).build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState, previousState));
        taskQueue.runAllRunnableTasks();

        assertThat(capturedCancellations, hasSize(1));
        assertThat(capturedCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, targetAllocationId.getId(), false)));
        assertThat(
            service.sentCancellations.get(targetAllocationId.getId()),
            equalTo(new RecoveryDirectCancellationService.SentCancellation(clusterState.term(), false))
        );
        capturedCancellations.clear();

        // Desired-balance path with cancelIfStarted=false, deduplicated
        service.cancelUndesiredRecoveries(desiredBalance, createRoutingAllocationFrom(clusterState));
        taskQueue.runAllRunnableTasks();

        assertThat(capturedCancellations, hasSize(0));

        // Desired-balance path with canRemain=NO produces cancelIfStarted=true
        service.cancelUndesiredRecoveries(
            desiredBalance,
            createRoutingAllocationFrom(clusterState, forbidRemainDecider(shardId, "node-1", true))
        );
        taskQueue.runAllRunnableTasks();

        assertThat(capturedCancellations, hasSize(1));
        assertThat(capturedCancellations.getFirst(), equalTo(new ShardRecoveryCancellation(shardId, targetAllocationId.getId(), true)));
        assertThat(
            service.sentCancellations.get(targetAllocationId.getId()),
            equalTo(new RecoveryDirectCancellationService.SentCancellation(clusterState.term(), true))
        );
        capturedCancellations.clear();

        // Snapshot path again with cancelIfStarted=false, deduplicated
        service.clusterChanged(new ClusterChangedEvent("test", clusterState, previousState));
        taskQueue.runAllRunnableTasks();
        assertThat(capturedCancellations, hasSize(0));
        assertThat(
            service.sentCancellations.get(targetAllocationId.getId()),
            equalTo(new RecoveryDirectCancellationService.SentCancellation(clusterState.term(), true))
        );
    }

    public void testRecoveryCancellationSkippedWhenRelocationDuringSnapshotEnabled() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var initShardId = new ShardId(index, 1);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(newShardRouting(initShardId, "node-2", true, STARTED));
        final var snapshot = snapshotWithShards(
            Map.of(
                waitingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                initShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var compatVersions = new CompatibilityVersions(TransportVersion.current(), Map.of());
        final var clusterState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .putCompatibilityVersions("node-0", compatVersions)
            .putCompatibilityVersions("node-1", compatVersions)
            .putCompatibilityVersions("node-2", compatVersions)
            .build();

        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());
        final var capturedCancellations = new CopyOnWriteArrayList<ShardRecoveryCancellation>();
        doAnswer(invocation -> {
            final CancelRecoveriesAction.Request req = invocation.getArgument(2);
            capturedCancellations.addAll(req.cancellations());
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var service = new RecoveryDirectCancellationService(
            transportService,
            createMockClusterService(clusterState, true, true),
            mock(AllocationService.class),
            mock(RerouteService.class)
        );
        service.start();

        final var previousState = ClusterState.builder(clusterState).removeCustom(SnapshotsInProgress.TYPE).build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState, previousState));
        taskQueue.runAllRunnableTasks();

        assertThat(capturedCancellations, hasSize(0));
    }

    public void testSnapshotCancellationRunsAreCoalesced() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var initShardId = new ShardId(index, 1);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            )
            .addShard(newShardRouting(initShardId, "node-2", true, STARTED));
        final var snapshot = snapshotWithShards(
            Map.of(
                waitingShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null),
                initShardId,
                new SnapshotsInProgress.ShardSnapshotStatus("node-2", SnapshotsInProgress.ShardState.INIT, null)
            )
        );
        final var compatVersions = new CompatibilityVersions(TransportVersion.current(), Map.of());
        final var initialState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .putCompatibilityVersions("node-0", compatVersions)
            .putCompatibilityVersions("node-1", compatVersions)
            .putCompatibilityVersions("node-2", compatVersions)
            .build();

        final var currentState = new AtomicReference<>(initialState);
        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());

        final var sendCount = new AtomicInteger();
        doAnswer(invocation -> {
            sendCount.incrementAndGet();
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var clusterService = createMockClusterService(initialState, true);
        when(clusterService.state()).thenAnswer(invocation -> currentState.get());

        final var service = new RecoveryDirectCancellationService(
            transportService,
            clusterService,
            mock(AllocationService.class),
            mock(RerouteService.class)
        );
        service.start();

        final var stateWithoutSnapshots = ClusterState.builder(initialState).removeCustom(SnapshotsInProgress.TYPE).build();
        service.clusterChanged(new ClusterChangedEvent("test", initialState, stateWithoutSnapshots));

        // Further triggers while the first run is still queued. Each gets a fresh routing-table instance so
        // clusterChanged's gate fires. Without coalescing each would schedule its own runnable.
        final int extraTriggers = randomIntBetween(2, 8);
        for (int i = 0; i < extraTriggers; i++) {
            final var previous = currentState.get();
            final var next = ClusterState.builder(previous)
                .version(previous.version() + 1)
                .routingTable(RoutingTable.builder(previous.routingTable()).build())
                .build();
            currentState.set(next);
            service.clusterChanged(new ClusterChangedEvent("test", next, previous));
        }

        int cancellationRuns = 0;
        while (taskQueue.hasRunnableTasks()) {
            cancellationRuns++;
            taskQueue.runRandomTask();
        }
        assertThat("concurrent snapshot-cancellation triggers should coalesce to a single queued run", cancellationRuns, equalTo(1));
        assertThat(sendCount.get(), equalTo(1));
        assertThat(
            service.sentCancellations.get(targetAllocationId.getId()),
            equalTo(new RecoveryDirectCancellationService.SentCancellation(currentState.get().term(), false))
        );
    }

    public void testSnapshotCancellationFollowUpScheduledWhileRunInFlight() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 0)).build();
        final var index = indexMetadata.getIndex();
        final var waitingShardId = new ShardId(index, 0);
        final var sourceAllocationId = AllocationId.newRelocation(AllocationId.newInitializing(randomIdentifier("source-")));
        final var targetAllocationId = AllocationId.newTargetRelocation(sourceAllocationId);

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(
                TestShardRouting.shardRoutingBuilder(waitingShardId, "node-0", true, RELOCATING)
                    .withAllocationId(sourceAllocationId)
                    .withRelocatingNodeId("node-1")
                    .build()
            );
        final var snapshot = snapshotWithShards(
            Map.of(waitingShardId, new SnapshotsInProgress.ShardSnapshotStatus("node-0", SnapshotsInProgress.ShardState.WAITING, null))
        );
        final var compatVersions = new CompatibilityVersions(TransportVersion.current(), Map.of());
        final var initialState = ClusterState.builder(clusterStateWithSnapshot(indexMetadata, indexRoutingTable, snapshot))
            .putCompatibilityVersions("node-0", compatVersions)
            .putCompatibilityVersions("node-1", compatVersions)
            .putCompatibilityVersions("node-2", compatVersions)
            .build();

        final var currentState = new AtomicReference<>(initialState);
        final var taskQueue = new DeterministicTaskQueue();
        final var transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(taskQueue.getThreadPool());

        final var clusterService = createMockClusterService(initialState, true);
        when(clusterService.state()).thenAnswer(invocation -> currentState.get());

        final var service = new RecoveryDirectCancellationService(
            transportService,
            clusterService,
            mock(AllocationService.class),
            mock(RerouteService.class)
        );
        service.start();

        final var sendCount = new AtomicInteger();
        doAnswer(invocation -> {
            final int sends = sendCount.incrementAndGet();
            if (sends == 1) {
                // First run has already released the schedule permit and is still executing. A new trigger should
                // schedule a follow-up rather than being coalesced.
                final var previous = currentState.get();
                final var next = ClusterState.builder(previous)
                    .version(previous.version() + 1)
                    .metadata(
                        Metadata.builder(previous.metadata())
                            .coordinationMetadata(
                                CoordinationMetadata.builder(previous.coordinationMetadata()).term(previous.term() + 1).build()
                            )
                            .build()
                    )
                    .routingTable(RoutingTable.builder(previous.routingTable()).build())
                    .build();
                currentState.set(next);
                service.clusterChanged(new ClusterChangedEvent("test", next, previous));
                assertTrue("trigger during an in-flight run should queue a follow-up", taskQueue.hasRunnableTasks());
            }
            final TransportResponseHandler<CancelRecoveriesAction.Response> handler = invocation.getArgument(3);
            handler.handleResponse(new CancelRecoveriesAction.Response(Set.of()));
            return null;
        }).when(transportService).sendRequest(any(DiscoveryNode.class), anyString(), any(), any());

        final var stateWithoutSnapshots = ClusterState.builder(initialState).removeCustom(SnapshotsInProgress.TYPE).build();
        service.clusterChanged(new ClusterChangedEvent("test", initialState, stateWithoutSnapshots));

        int cancellationRuns = 0;
        while (taskQueue.hasRunnableTasks()) {
            cancellationRuns++;
            taskQueue.runRandomTask();
        }
        assertThat("a trigger after the first run starts should schedule a second run", cancellationRuns, equalTo(2));
        // Term bump on the follow-up state bypasses the cache so both runs issue a cancellation.
        assertThat(sendCount.get(), equalTo(2));
        assertThat(
            service.sentCancellations.get(targetAllocationId.getId()),
            equalTo(new RecoveryDirectCancellationService.SentCancellation(currentState.get().term(), false))
        );
    }

    private SnapshotsInProgress.Entry snapshotWithShards(Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards) {
        final var snapshot = new Snapshot("test-repo", new SnapshotId("test-snapshot", randomIdentifier()));
        assertThat(
            "all provided shards should have be from the same index",
            shards.keySet().stream().map(ShardId::getIndexName).distinct().count(),
            equalTo(1L)
        );
        final var indexName = shards.keySet().stream().findAny().get().getIndexName();
        final var indexId = new IndexId(indexName, randomIdentifier());
        return SnapshotsInProgress.Entry.snapshot(
            snapshot,
            false,
            false,
            SnapshotsInProgress.State.STARTED,
            Map.of(indexName, indexId),
            List.of(),
            List.of(),
            0L,
            -1L,
            shards,
            null,
            null,
            IndexVersion.current()
        );
    }

    private ClusterState clusterStateWithSnapshot(
        IndexMetadata indexMetadata,
        IndexRoutingTable.Builder indexRoutingTable,
        SnapshotsInProgress.Entry snapshot
    ) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(snapshot))
            .build();
    }

    private SingleNodeShutdownMetadata nodeShutdownMetadata(String nodeId, SingleNodeShutdownMetadata.Type type) {
        final var builder = SingleNodeShutdownMetadata.builder().setNodeId(nodeId).setType(type).setReason("test").setStartedAtMillis(0L);
        switch (type) {
            case REPLACE -> builder.setTargetNodeName(randomIdentifier("target-"));
            case SIGTERM -> builder.setGracePeriod(randomPositiveTimeValue());
            case REMOVE, RESTART -> {
            }
        }
        return builder.build();
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
        return createMockClusterService(clusterState, enableDirectCancellations, false);
    }

    private ClusterService createMockClusterService(
        ClusterState clusterState,
        boolean enableDirectCancellations,
        boolean relocationDuringSnapshotEnabled
    ) {
        final Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING);
        final var relocationDuringSnapshotSetting = Setting.boolSetting(
            SnapshotInProgressAllocationDecider.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING_NAME,
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        settingSet.add(relocationDuringSnapshotSetting);
        final var initialSettings = Settings.builder()
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), enableDirectCancellations)
            .put(relocationDuringSnapshotSetting.getKey(), relocationDuringSnapshotEnabled)
            .build();
        final var clusterSettings = new ClusterSettings(initialSettings, settingSet);

        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
        doReturn(mock(MasterServiceTaskQueue.class)).when(clusterService).createTaskQueue(anyString(), any(Priority.class), any());
        doNothing().when(clusterService).addListener(any());
        doNothing().when(clusterService).removeListener(any());
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
