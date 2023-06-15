/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeReplacementAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.NodeShutdownAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.test.MockLogAppender.assertThatLogger;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DesiredBalanceReconcilerTests extends ESAllocationTestCase {

    public void testNoChangesOnEmptyDesiredBalance() {
        final var clusterState = DesiredBalanceComputerTests.createInitialClusterState(3);
        final var routingAllocation = createRoutingAllocationFrom(clusterState);

        reconcile(routingAllocation, new DesiredBalance(1, Map.of()));
        assertFalse(routingAllocation.routingNodesChanged());
    }

    public void testFailsNewPrimariesIfNoDataNodes() {
        final var clusterState = ClusterState.builder(DesiredBalanceComputerTests.createInitialClusterState(3))
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("master", Set.of(DiscoveryNodeRole.MASTER_ROLE)))
                    .localNodeId("master")
                    .masterNodeId("master")
                    .build()
            )
            .build();

        final var routingNodes = clusterState.mutableRoutingNodes();
        final var unassigned = routingNodes.unassigned().iterator();
        while (unassigned.hasNext()) {
            final var shardRouting = unassigned.next();
            if (shardRouting.primary() && shardRouting.shardId().id() == 1) {
                final var unassignedInfo = shardRouting.unassignedInfo();
                assertThat(unassignedInfo.getLastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.NO_ATTEMPT));
                unassigned.updateUnassigned(
                    new UnassignedInfo(
                        unassignedInfo.getReason(),
                        unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(),
                        unassignedInfo.getNumFailedAllocations(),
                        unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(),
                        unassignedInfo.isDelayed(),
                        UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED,
                        unassignedInfo.getFailedNodeIds(),
                        unassignedInfo.getLastAllocatedNodeId()
                    ),
                    shardRouting.recoverySource(),
                    new RoutingChangesObserver.DelegatingRoutingChangesObserver()
                );
            }
        }

        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            routingNodes,
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );

        for (ShardRouting shardRouting : routingAllocation.routingNodes().unassigned()) {
            assertTrue(shardRouting.toString(), shardRouting.unassigned());
            assertThat(
                shardRouting.unassignedInfo().getLastAllocationStatus(),
                equalTo(
                    shardRouting.primary() && shardRouting.shardId().id() == 1
                        ? UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED
                        : UnassignedInfo.AllocationStatus.NO_ATTEMPT
                )
            );
        }

        reconcile(
            routingAllocation,
            new DesiredBalance(
                1,
                randomBoolean()
                    ? Map.of()
                    : Map.of(
                        new ShardId(clusterState.metadata().index(DesiredBalanceComputerTests.TEST_INDEX).getIndex(), 0),
                        new ShardAssignment(Set.of("node-0"), 1, 0, 0)
                    )
            )
        );
        assertTrue(routingAllocation.routingNodesChanged());

        for (ShardRouting shardRouting : routingAllocation.routingNodes().unassigned()) {
            assertTrue(shardRouting.toString(), shardRouting.unassigned());
            assertThat(
                shardRouting.unassignedInfo().getLastAllocationStatus(),
                equalTo(
                    // we only update primaries, and only if currently NO_ATTEMPT
                    shardRouting.primary()
                        ? shardRouting.shardId().id() == 1
                            ? UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED
                            : UnassignedInfo.AllocationStatus.DECIDERS_NO
                        : UnassignedInfo.AllocationStatus.NO_ATTEMPT
                )
            );
        }
    }

    public void testUnassignedPrimariesBeforeUnassignedReplicas() {
        // regardless of priority, we attempt to allocate all unassigned primaries before considering any unassigned replicas

        final var discoveryNodes = discoveryNodes(2);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata0 = randomPriorityIndex("index-0", 1, 1);
        metadata.put(indexMetadata0, true);
        routingTable.addAsNew(indexMetadata0);

        final var indexMetadata1 = randomPriorityIndex("index-1", 1, 1);
        metadata.put(indexMetadata1, true);
        routingTable.addAsNew(indexMetadata1);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = throttleSettings();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var allocationFilter = new AtomicReference<BiPredicate<String, String>>(
            (indexName, nodeId) -> indexName.equals("index-0") && nodeId.equals("node-0")
        );

        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return allocationFilter.get().test(shardRouting.getIndexName(), node.nodeId()) ? Decision.YES : Decision.NO;
                }
            }
        );

        // first start the primary of index-0 (no other shards may be allocated due to allocation filter)
        final var stateWithStartedPrimary = startInitializingShardsAndReroute(
            allocationService,
            startInitializingShardsAndReroute(allocationService, clusterState)
        );
        {
            final var index0RoutingTable = stateWithStartedPrimary.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(index0RoutingTable.primaryShard().started());
            assertTrue(index0RoutingTable.replicaShards().stream().allMatch(ShardRouting::unassigned));
            final var index1RoutingTable = stateWithStartedPrimary.routingTable().shardRoutingTable("index-1", 0);
            assertTrue(index1RoutingTable.primaryShard().unassigned());
            assertTrue(index1RoutingTable.replicaShards().stream().allMatch(ShardRouting::unassigned));
        }

        // now relax the filter so that the replica of index-0 and the primary of index-1 can both be assigned to node-1, but the throttle
        // forces us to choose one of them to go first which must be the primary
        allocationFilter.set((indexName, nodeId) -> indexName.equals("index-0") || nodeId.equals("node-1"));
        final var stateWithInitializingSecondPrimary = startInitializingShardsAndReroute(allocationService, stateWithStartedPrimary);
        {
            final var index0RoutingTable = stateWithInitializingSecondPrimary.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(index0RoutingTable.primaryShard().started());
            assertTrue(index0RoutingTable.replicaShards().stream().allMatch(ShardRouting::unassigned));
            final var index1RoutingTable = stateWithInitializingSecondPrimary.routingTable().shardRoutingTable("index-1", 0);
            assertTrue(index1RoutingTable.primaryShard().initializing());
            assertTrue(index1RoutingTable.replicaShards().stream().allMatch(ShardRouting::unassigned));
        }

        final var stateWithStartedPrimariesAndInitializingReplica = startInitializingShardsAndReroute(
            allocationService,
            stateWithInitializingSecondPrimary
        );
        {
            final var index0RoutingTable = stateWithStartedPrimariesAndInitializingReplica.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(index0RoutingTable.primaryShard().started());
            assertTrue(index0RoutingTable.replicaShards().stream().allMatch(ShardRouting::initializing));
            final var index1RoutingTable = stateWithStartedPrimariesAndInitializingReplica.routingTable().shardRoutingTable("index-1", 0);
            assertTrue(index1RoutingTable.primaryShard().started());
            assertTrue(index1RoutingTable.replicaShards().stream().allMatch(ShardRouting::unassigned));
        }
    }

    public void testUnassignedShardsInterleaving() {
        // regardless of priority, we give each shard an opportunity to allocate one of its copies before we give any shard an opportunity
        // to allocate a further copy

        final var discoveryNodes = discoveryNodes(4);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        var shardsRemaining = 4;
        var indexNum = 0;
        while (shardsRemaining > 0) {
            final var shardCount = between(1, shardsRemaining);
            shardsRemaining -= shardCount;
            final var indexMetadata = randomPriorityIndex("index-" + indexNum++, shardCount, 3);
            metadata.put(indexMetadata, true);
            routingTable.addAsNew(indexMetadata);
        }

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = throttleSettings();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);

        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings)
        );

        final var stateWithInitializingPrimaries = startInitializingShardsAndReroute(allocationService, clusterState);
        for (final var indexRoutingTable : stateWithInitializingPrimaries.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(indexShardRoutingTable.primaryShard().initializing());
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::unassigned).count(), equalTo(3L));
            }
        }

        final var stateWithInitializingReplicas1 = startInitializingShardsAndReroute(allocationService, stateWithInitializingPrimaries);
        for (final var indexRoutingTable : stateWithInitializingReplicas1.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(indexShardRoutingTable.primaryShard().started());
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::unassigned).count(), equalTo(2L));
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::initializing).count(), equalTo(1L));
            }
        }

        final var stateWithInitializingReplicas2 = startInitializingShardsAndReroute(allocationService, stateWithInitializingReplicas1);
        for (final var indexRoutingTable : stateWithInitializingReplicas2.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(indexShardRoutingTable.primaryShard().started());
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::unassigned).count(), equalTo(1L));
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::initializing).count(), equalTo(1L));
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::started).count(), equalTo(1L));
            }
        }

        final var stateWithInitializingReplicas3 = startInitializingShardsAndReroute(allocationService, stateWithInitializingReplicas2);
        for (final var indexRoutingTable : stateWithInitializingReplicas3.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(indexShardRoutingTable.primaryShard().started());
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::initializing).count(), equalTo(1L));
                assertThat(indexShardRoutingTable.replicaShards().stream().filter(ShardRouting::started).count(), equalTo(2L));
            }
        }

        final var finalState = startInitializingShardsAndReroute(allocationService, stateWithInitializingReplicas3);
        for (final var indexRoutingTable : finalState.routingTable()) {
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertTrue(indexShardRoutingTable.allShardsStarted());
            }
        }
    }

    public void testUnassignedShardsPriority() {
        final var discoveryNodes = discoveryNodes(2);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata0 = randomPriorityIndex("index-0", 2, 1);
        final var indexMetadata1 = randomPriorityIndex("index-1", 2, 1);

        metadata.put(indexMetadata0, true);
        metadata.put(indexMetadata1, true);
        routingTable.addAsNew(indexMetadata0);
        routingTable.addAsNew(indexMetadata1);

        final var comparisonResult = Comparator.<IndexMetadata>comparingInt(indexMetadata -> indexMetadata.isSystem() ? 1 : 0)
            .thenComparingInt(IndexMetadata::priority)
            .thenComparingLong(IndexMetadata::getCreationDate)
            .thenComparing(indexMetadata -> indexMetadata.getIndex().getName())
            .compare(indexMetadata0, indexMetadata1);
        assert comparisonResult != 0;
        final var higherIndex = comparisonResult > 0 ? indexMetadata0 : indexMetadata1;
        final var lowerIndex = comparisonResult > 0 ? indexMetadata1 : indexMetadata0;

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = throttleSettings();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var assignReplicas = new AtomicBoolean(false);

        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return (shardRouting.primary() && node.nodeId().equals("node-0")) || assignReplicas.get() ? Decision.YES : Decision.NO;
                }
            }
        );

        final TriFunction<ClusterState, IndexMetadata, Integer, ShardRouting> primaryGetter = (state, indexMetadata, shardId) -> state
            .routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId)
            .primaryShard();

        final var state1 = startInitializingShardsAndReroute(allocationService, clusterState);
        assertTrue(primaryGetter.apply(state1, higherIndex, 0).initializing());
        assertTrue(primaryGetter.apply(state1, higherIndex, 1).unassigned());
        assertTrue(primaryGetter.apply(state1, lowerIndex, 0).unassigned());
        assertTrue(primaryGetter.apply(state1, lowerIndex, 1).unassigned());

        final var state2 = startInitializingShardsAndReroute(allocationService, state1);
        assertTrue(primaryGetter.apply(state2, higherIndex, 0).started());
        assertTrue(primaryGetter.apply(state2, higherIndex, 1).initializing());
        assertTrue(primaryGetter.apply(state2, lowerIndex, 0).unassigned());
        assertTrue(primaryGetter.apply(state2, lowerIndex, 1).unassigned());

        final var state3 = startInitializingShardsAndReroute(allocationService, state2);
        assertTrue(primaryGetter.apply(state3, higherIndex, 0).started());
        assertTrue(primaryGetter.apply(state3, higherIndex, 1).started());
        assertTrue(primaryGetter.apply(state3, lowerIndex, 0).initializing());
        assertTrue(primaryGetter.apply(state3, lowerIndex, 1).unassigned());

        final var state4 = startInitializingShardsAndReroute(allocationService, state3);
        assertTrue(primaryGetter.apply(state4, higherIndex, 0).started());
        assertTrue(primaryGetter.apply(state4, higherIndex, 1).started());
        assertTrue(primaryGetter.apply(state4, lowerIndex, 0).started());
        assertTrue(primaryGetter.apply(state4, lowerIndex, 1).initializing());

        final var state5 = startInitializingShardsAndReroute(allocationService, state4);
        assertTrue(primaryGetter.apply(state5, higherIndex, 0).started());
        assertTrue(primaryGetter.apply(state5, higherIndex, 1).started());
        assertTrue(primaryGetter.apply(state5, lowerIndex, 0).started());
        assertTrue(primaryGetter.apply(state5, lowerIndex, 1).started());

        final TriFunction<ClusterState, IndexMetadata, Integer, ShardRouting> replicaGetter = (state, indexMetadata, shardId) -> state
            .routingTable()
            .shardRoutingTable(indexMetadata.getIndex().getName(), shardId)
            .replicaShards()
            .get(0);

        assignReplicas.set(true);

        final var state6 = startInitializingShardsAndReroute(allocationService, state5);
        assertTrue(replicaGetter.apply(state6, higherIndex, 0).initializing());
        assertTrue(replicaGetter.apply(state6, higherIndex, 1).unassigned());
        assertTrue(replicaGetter.apply(state6, lowerIndex, 0).unassigned());
        assertTrue(replicaGetter.apply(state6, lowerIndex, 1).unassigned());

        final var state7 = startInitializingShardsAndReroute(allocationService, state6);
        assertTrue(replicaGetter.apply(state7, higherIndex, 0).started());
        assertTrue(replicaGetter.apply(state7, higherIndex, 1).initializing());
        assertTrue(replicaGetter.apply(state7, lowerIndex, 0).unassigned());
        assertTrue(replicaGetter.apply(state7, lowerIndex, 1).unassigned());

        final var state8 = startInitializingShardsAndReroute(allocationService, state7);
        assertTrue(replicaGetter.apply(state8, higherIndex, 0).started());
        assertTrue(replicaGetter.apply(state8, higherIndex, 1).started());
        assertTrue(replicaGetter.apply(state8, lowerIndex, 0).initializing());
        assertTrue(replicaGetter.apply(state8, lowerIndex, 1).unassigned());

        final var state9 = startInitializingShardsAndReroute(allocationService, state8);
        assertTrue(replicaGetter.apply(state9, higherIndex, 0).started());
        assertTrue(replicaGetter.apply(state9, higherIndex, 1).started());
        assertTrue(replicaGetter.apply(state9, lowerIndex, 0).started());
        assertTrue(replicaGetter.apply(state9, lowerIndex, 1).initializing());

        final var state10 = startInitializingShardsAndReroute(allocationService, state9);
        assertTrue(replicaGetter.apply(state10, higherIndex, 0).started());
        assertTrue(replicaGetter.apply(state10, higherIndex, 1).started());
        assertTrue(replicaGetter.apply(state10, lowerIndex, 0).started());
        assertTrue(replicaGetter.apply(state10, lowerIndex, 1).started());
    }

    public void testUnassignedRespectsDesiredBalance() {
        final var discoveryNodes = discoveryNodes(5);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        for (var i = 0; i < 5; i++) {
            final var indexMetadata = randomPriorityIndex("index-" + i, between(1, 5), between(0, 4));
            metadata.put(indexMetadata, true);
            routingTable.addAsNew(indexMetadata);
        }

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        );

        ClusterState reroutedState = clusterState;
        var changed = true;
        while (changed) {
            final var nextState = startInitializingShardsAndReroute(allocationService, reroutedState);
            changed = nextState != reroutedState;
            reroutedState = nextState;
        }

        boolean anyAssigned = false;
        for (final var indexRoutingTable : reroutedState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(shardId);
                final var nodeIds = new HashSet<String>();
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    final var shardRouting = indexShardRoutingTable.shard(copy);
                    if (shardRouting.started()) {
                        anyAssigned = true;
                        nodeIds.add(shardRouting.currentNodeId());
                    } else {
                        assertTrue(shardRouting.unassigned());
                    }
                }
                assertTrue(desiredBalance.getAssignment(indexShardRoutingTable.shardId()).nodeIds().containsAll(nodeIds));
            }
        }

        assertNotEquals(anyAssigned, desiredBalance.assignments().values().stream().map(ShardAssignment::nodeIds).allMatch(Set::isEmpty));
    }

    public void testUnassignedAllocationPredictsDiskUsage() {
        final var discoveryNodes = discoveryNodes(1);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var existingIndexMetadata = randomPriorityIndex("index-existing", 1, 0);
        metadata.put(existingIndexMetadata, true);
        routingTable.addAsRecovery(existingIndexMetadata);

        final var restoredIndexMetadata = randomPriorityIndex("index-restored", 1, 0);
        metadata.put(restoredIndexMetadata, true);
        final var recoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(random()),
            new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID(random()))),
            IndexVersion.CURRENT,
            new IndexId("index", UUIDs.randomBase64UUID(random()))
        );
        routingTable.addAsRestore(restoredIndexMetadata, recoverySource);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var existingShardSize = randomNonNegativeLong();
        final var shardSizesBuilder = ImmutableOpenMap.<String, Long>builder();
        shardSizesBuilder.put(
            shardIdentifierFromRouting(clusterState.routingTable().shardRoutingTable("index-existing", 0).primaryShard()),
            existingShardSize
        );
        final var clusterInfo = new ClusterInfo(
            ImmutableOpenMap.of(),
            ImmutableOpenMap.of(),
            shardSizesBuilder.build(),
            ImmutableOpenMap.of(),
            ImmutableOpenMap.of(),
            ImmutableOpenMap.of()
        );

        final var restoredShardSize = randomNonNegativeLong();
        final var snapshotSizesBuilder = ImmutableOpenMap.<InternalSnapshotsInfoService.SnapshotShard, Long>builder();
        snapshotSizesBuilder.put(
            new InternalSnapshotsInfoService.SnapshotShard(
                recoverySource.snapshot(),
                recoverySource.index(),
                new ShardId(restoredIndexMetadata.getIndex(), 0)
            ),
            restoredShardSize
        );
        final var snapshotShardSizeInfo = new SnapshotShardSizeInfo(snapshotSizesBuilder.build());

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            () -> clusterInfo,
            () -> snapshotShardSizeInfo,
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        );

        final var reroutedState = allocationService.reroute(clusterState, "test", ActionListener.noop());

        final var existingShard = reroutedState.routingTable().shardRoutingTable("index-existing", 0).primaryShard();
        assertTrue(existingShard.initializing());
        assertThat(existingShard.getExpectedShardSize(), equalTo(existingShardSize));

        final var restoredShard = reroutedState.routingTable().shardRoutingTable("index-restored", 0).primaryShard();
        assertTrue(restoredShard.initializing());
        assertThat(restoredShard.getExpectedShardSize(), equalTo(restoredShardSize));
    }

    public void testUnassignedSkipsEquivalentReplicas() {
        final var discoveryNodes = discoveryNodes(2);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata = randomPriorityIndex("index-0", 1, between(0, 5));
        metadata.put(indexMetadata, true);
        routingTable.addAsNew(indexMetadata);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var triedReplica = new AtomicBoolean();
        final var replicaDecision = randomFrom(Decision.THROTTLE, Decision.NO);
        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    if (shardRouting.primary()) {
                        return Decision.YES;
                    } else {
                        // there are two replicas but they're equivalent so we should only call canAllocate once.
                        assert triedReplica.compareAndSet(false, true);
                        return replicaDecision;
                    }
                }
            }
        );

        var reroutedState = clusterState;
        boolean changed;
        do {
            triedReplica.set(false);
            final var newState = startInitializingShardsAndReroute(allocationService, reroutedState);
            changed = newState != reroutedState;
            reroutedState = newState;
        } while (changed);

        assertTrue(
            reroutedState.routingTable()
                .shardRoutingTable("index-0", 0)
                .replicaShards()
                .stream()
                .allMatch(
                    shardRouting -> shardRouting.unassignedInfo().getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT
                )
        );
    }

    public void testUnassignedSetsAllocationStatusOnUnassignedShards() {
        final var discoveryNodes = discoveryNodes(2);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata = randomPriorityIndex("index-0", 1, between(0, 5));
        metadata.put(indexMetadata, true);
        routingTable.addAsNew(indexMetadata);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var assignPrimary = new AtomicBoolean(false);
        final var nonYesDecision = randomFrom(Decision.THROTTLE, Decision.NO);
        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> true);
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    if (shardRouting.primary()) {
                        return assignPrimary.get() ? Decision.YES : nonYesDecision;
                    } else {
                        return nonYesDecision;
                    }
                }
            }
        );

        final var redState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertEquals(
            nonYesDecision == Decision.NO
                ? UnassignedInfo.AllocationStatus.DECIDERS_NO
                : UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED,
            redState.routingTable().shardRoutingTable("index-0", 0).primaryShard().unassignedInfo().getLastAllocationStatus()
        );

        assignPrimary.set(true);
        final var yellowState = startInitializingShardsAndReroute(
            allocationService,
            startInitializingShardsAndReroute(allocationService, redState)
        );
        for (final var shardRouting : yellowState.routingTable().shardRoutingTable("index-0", 0).replicaShards()) {
            assertEquals(UnassignedInfo.AllocationStatus.NO_ATTEMPT, shardRouting.unassignedInfo().getLastAllocationStatus());
        }
    }

    public void testUnassignedPrimariesThrottlingAndFallback() {
        // we fall back to trying all nodes if an unassigned primary cannot be assigned to a desired node, but only if the desired nodes
        // aren't just throttled

        final var discoveryNodes = discoveryNodes(2);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata0 = randomPriorityIndex("index-0", 2, 0);
        metadata.put(indexMetadata0, true);
        routingTable.addAsNew(indexMetadata0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = throttleSettings();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var desiredBalance = desiredBalance(clusterState, (shardId, nodeId) -> nodeId.equals("node-0"));
        final var allocationFilter = new AtomicReference<BiPredicate<Integer, String>>();

        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings),
            new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return allocationFilter.get().test(shardRouting.getId(), node.nodeId()) ? Decision.YES : Decision.NO;
                }
            }
        );

        final var unused = ActionListener.<Void>noop();

        // first assign the primary of [index-0][0] (no other shards may be allocated due to allocation filter)
        allocationFilter.set((shardId, nodeId) -> shardId == 0);
        final var stateWithOneInitializingPrimary = allocationService.reroute(clusterState, "test", unused);
        {
            final var shard0RoutingTable = stateWithOneInitializingPrimary.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(shard0RoutingTable.primaryShard().initializing());
            assertThat(shard0RoutingTable.primaryShard().currentNodeId(), equalTo("node-0"));
            final var shard1RoutingTable = stateWithOneInitializingPrimary.routingTable().shardRoutingTable("index-0", 1);
            assertTrue(shard1RoutingTable.primaryShard().unassigned());
        }

        // now relax the allocation filter and ensure that [index-0][1] still isn't assigned due to throttling on the desired node
        allocationFilter.set((shardId, nodeId) -> true);
        final var stateStillWithOneInitializingPrimary = allocationService.reroute(stateWithOneInitializingPrimary, "test", unused);
        {
            final var shard0RoutingTable = stateStillWithOneInitializingPrimary.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(shard0RoutingTable.primaryShard().initializing());
            assertThat(shard0RoutingTable.primaryShard().currentNodeId(), equalTo("node-0"));
            final var shard1RoutingTable = stateStillWithOneInitializingPrimary.routingTable().shardRoutingTable("index-0", 1);
            assertTrue(shard1RoutingTable.primaryShard().unassigned());
        }

        // now forbid [index-0][1] from its desired node and see that it falls back to the undesired node
        allocationFilter.set((shardId, nodeId) -> nodeId.equals("node-1"));
        final var stateWithBothInitializingPrimaries = allocationService.reroute(stateStillWithOneInitializingPrimary, "test", unused);
        {
            final var shard0RoutingTable = stateWithBothInitializingPrimaries.routingTable().shardRoutingTable("index-0", 0);
            assertTrue(shard0RoutingTable.primaryShard().initializing());
            assertThat(shard0RoutingTable.primaryShard().currentNodeId(), equalTo("node-0"));
            final var shard1RoutingTable = stateWithBothInitializingPrimaries.routingTable().shardRoutingTable("index-0", 1);
            assertTrue(shard1RoutingTable.primaryShard().initializing());
            assertThat(shard1RoutingTable.primaryShard().currentNodeId(), equalTo("node-1"));
        }
    }

    public void testMoveShards() {
        final var discoveryNodes = discoveryNodes(4);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata = randomPriorityIndex("index-0", 3, 1);
        metadata.put(indexMetadata, true);
        routingTable.addAsNew(indexMetadata);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = Settings.builder()
            .put(throttleSettings())
            .putList(
                FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey(),
                "node-0",
                "node-1"
            )
            .build();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var canAllocateRef = new AtomicReference<>(Decision.YES);

        final var desiredBalance = new AtomicReference<>(desiredBalance(clusterState, (shardId, nodeId) -> true));
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance.get()),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings),
            new FilterAllocationDecider(settings, clusterSettings),
            new NodeShutdownAllocationDecider(),
            new NodeReplacementAllocationDecider(),
            new AllocationDecider() {
                @Override
                public Decision canRebalance(RoutingAllocation allocation) {
                    return Decision.NO;
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                    return canAllocateRef.get();
                }
            }
        );

        boolean changed;
        do {
            final var newState = startInitializingShardsAndReroute(allocationService, clusterState);
            changed = newState != clusterState;
            clusterState = newState;
        } while (changed);
        for (ShardRouting shardRouting : clusterState.routingTable().allShardsIterator()) {
            assertTrue(shardRouting.started());
            assertThat(shardRouting.currentNodeId(), oneOf("node-0", "node-1"));
        }

        clusterSettings.applySettings(
            Settings.builder()
                .putList(
                    FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey(),
                    "node-2",
                    "node-3"
                )
                .build()
        );

        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop())); // all still on desired nodes, no
                                                                                                          // movement needed

        desiredBalance.set(desiredBalance(clusterState, (shardId, nodeId) -> nodeId.equals("node-2") || nodeId.equals("node-3")));

        // The next reroute starts moving shards to node-2 and node-3, but interleaves the decisions between node-0 and node-1 for fairness.
        // There's an inbound throttle of 1 but no outbound throttle, so without the interleaving one node would relocate 2 shards.
        final var reroutedState = allocationService.reroute(clusterState, "test", ActionListener.noop());
        assertThat(reroutedState.getRoutingNodes().node("node-0").numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));
        assertThat(reroutedState.getRoutingNodes().node("node-1").numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));

        // Ensuring that we check the shortcut two-param canAllocate() method up front
        canAllocateRef.set(Decision.NO);
        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop()));
        canAllocateRef.set(Decision.YES);

        // Restore filter to default
        clusterSettings.applySettings(
            Settings.builder()
                .putList(
                    FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey(),
                    "node-0",
                    "node-1"
                )
                .build()
        );

        // Mark node-0 as shutting down, to be replaced by node-2, so that a shard can be force-moved to node-2 even though the allocation
        // filter forbids this
        final var shuttingDownState = allocationService.reroute(
            clusterState.copyAndUpdateMetadata(
                tmpMetadata -> tmpMetadata.putCustom(
                    NodesShutdownMetadata.TYPE,
                    new NodesShutdownMetadata(
                        Map.of(
                            "node-0",
                            SingleNodeShutdownMetadata.builder()
                                .setNodeId("node-0")
                                .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                                .setTargetNodeName("node-2")
                                .setStartedAtMillis(System.currentTimeMillis())
                                .setReason("test")
                                .build()
                        )
                    )
                )
            ),
            "test",
            ActionListener.noop()
        );
        assertThat(shuttingDownState.getRoutingNodes().node("node-2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
    }

    public void testRebalance() {
        final var discoveryNodes = discoveryNodes(4);
        final var metadata = Metadata.builder();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        final var indexMetadata = randomPriorityIndex("index-0", 3, 1);
        metadata.put(indexMetadata, true);
        routingTable.addAsNew(indexMetadata);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        final var settings = throttleSettings();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var canAllocateShardRef = new AtomicReference<>(Decision.YES);
        final var canRebalanceGlobalRef = new AtomicReference<>(Decision.YES);
        final var canRebalanceShardRef = new AtomicReference<>(Decision.YES);

        final var desiredBalance = new AtomicReference<>(
            desiredBalance(clusterState, (shardId, nodeId) -> nodeId.equals("node-0") || nodeId.equals("node-1"))
        );
        final var allocationService = createTestAllocationService(
            routingAllocation -> reconcile(routingAllocation, desiredBalance.get()),
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new ThrottlingAllocationDecider(clusterSettings),
            new AllocationDecider() {
                @Override
                public Decision canRebalance(RoutingAllocation allocation) {
                    return canRebalanceGlobalRef.get();
                }

                @Override
                public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
                    return canRebalanceShardRef.get();
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                    return canAllocateShardRef.get();
                }
            }
        );

        boolean changed;
        do {
            final var newState = startInitializingShardsAndReroute(allocationService, clusterState);
            changed = newState != clusterState;
            clusterState = newState;
        } while (changed);
        for (ShardRouting shardRouting : clusterState.routingTable().allShardsIterator()) {
            assertTrue(shardRouting.started());
            assertThat(shardRouting.currentNodeId(), oneOf("node-0", "node-1"));
        }

        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop())); // all still on desired nodes, no
                                                                                                          // movement needed

        desiredBalance.set(desiredBalance(clusterState, (shardId, nodeId) -> nodeId.equals("node-2") || nodeId.equals("node-3")));

        canRebalanceGlobalRef.set(Decision.NO);
        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop())); // rebalancing forbidden on all
                                                                                                          // shards, no movement
        canRebalanceGlobalRef.set(Decision.YES);

        canRebalanceShardRef.set(Decision.NO);
        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop())); // rebalancing forbidden on
                                                                                                          // specific shards, no movement
        canRebalanceShardRef.set(Decision.YES);

        canAllocateShardRef.set(Decision.NO);
        assertSame(clusterState, allocationService.reroute(clusterState, "test", ActionListener.noop())); // allocation not possible, no
                                                                                                          // movement
        canAllocateShardRef.set(Decision.YES);

        // The next reroute starts moving shards to node-2 and node-3, but interleaves the decisions between node-0 and node-1 for fairness.
        // There's an inbound throttle of 1 but no outbound throttle, so without the interleaving one node would relocate 2 shards.
        final var reroutedState = allocationService.reroute(clusterState, "test", ActionListener.noop());
        assertThat(reroutedState.getRoutingNodes().node("node-0").numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));
        assertThat(reroutedState.getRoutingNodes().node("node-1").numberOfShardsWithState(ShardRoutingState.RELOCATING), equalTo(1));
    }

    public void testDoNotRebalanceToTheNodeThatNoLongerExists() {

        var indexMetadata = IndexMetadata.builder("index-1").settings(indexSettings(Version.CURRENT, 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    // data-node-1 left the cluster
                    .localNodeId("data-node-2")
                    .masterNodeId("data-node-2")
                    .add(newNode("data-node-2"))
            )
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(index).addShard(newShardRouting(shardId, "data-node-2", true, STARTED)))
            )
            .build();

        final var allocation = createRoutingAllocationFrom(clusterState);
        final var balance = new DesiredBalance(
            1,
            Map.of(shardId, new ShardAssignment(Set.of("data-node-1"), 1, 0, 0)) // shard is assigned to the node that has left
        );

        reconcile(allocation, balance);

        assertThat(allocation.routingNodes().node("data-node-1"), nullValue());
        assertThat(allocation.routingNodes().node("data-node-2"), notNullValue());
        // shard is kept wherever until balance is recalculated
        assertThat(allocation.routingNodes().node("data-node-2").getByShardId(shardId), notNullValue());
    }

    public void testRebalanceDoesNotCauseHotSpots() {

        int numberOfNodes = randomIntBetween(5, 9);
        int shardsPerNode = randomIntBetween(1, 15);

        var indexMetadata = IndexMetadata.builder("index-1")
            .settings(indexSettings(Version.CURRENT, shardsPerNode * numberOfNodes, 0))
            .build();
        final var index = indexMetadata.getIndex();

        var discoveryNodeBuilder = DiscoveryNodes.builder().localNodeId("node-0").masterNodeId("node-0");
        for (int n = 0; n < numberOfNodes; n++) {
            discoveryNodeBuilder.add(newNode("node-" + n));
        }

        var assignments = new HashMap<ShardId, ShardAssignment>();
        var indexRoutingTableBuilder = IndexRoutingTable.builder(index);

        for (int i = 0; i < shardsPerNode * numberOfNodes; i++) {
            var shardId = new ShardId(index, i);

            var desiredNode = i % numberOfNodes;
            var currentNode = randomIntBetween(0, numberOfNodes - 1);

            assignments.put(shardId, new ShardAssignment(Set.of("node-" + desiredNode), 1, 0, 0));
            indexRoutingTableBuilder.addShard(newShardRouting(shardId, "node-" + currentNode, true, STARTED));
        }

        var balance = new DesiredBalance(1, assignments);
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodeBuilder)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTableBuilder))
            .build();

        var clusterSettings = createBuiltInClusterSettings();
        var deciders = new AllocationDecider[] {
            new ConcurrentRebalanceAllocationDecider(clusterSettings),
            new ThrottlingAllocationDecider(clusterSettings) };

        var reconciler = new DesiredBalanceReconciler(clusterSettings, mock(ThreadPool.class));

        var totalOutgoingMoves = new HashMap<String, AtomicInteger>();
        for (int i = 0; i < numberOfNodes; i++) {
            var nodeId = "node-" + i;
            if (isReconciled(clusterState.getRoutingNodes().node(nodeId), balance) == false) {
                totalOutgoingMoves.put(nodeId, new AtomicInteger());
            }
        }

        while (true) {

            var allocation = createRoutingAllocationFrom(clusterState, deciders);
            reconciler.reconcile(balance, allocation);

            var initializing = shardsWithState(allocation.routingNodes(), ShardRoutingState.INITIALIZING);
            if (initializing.isEmpty()) {
                break;
            }
            for (ShardRouting shardRouting : initializing) {
                totalOutgoingMoves.get(shardRouting.relocatingNodeId()).incrementAndGet();
                allocation.routingNodes().startShard(logger, shardRouting, allocation.changes(), 0L);
            }

            var summary = totalOutgoingMoves.values().stream().mapToInt(AtomicInteger::get).summaryStatistics();
            // ensure that we do not cause hotspots by round-robin unreconciled source nodes when picking next rebalance
            // (already reconciled nodes are excluded as they are no longer causing new moves)
            assertThat(
                "Reconciling nodes should all have same amount (max 1 delta) of moves: " + totalOutgoingMoves,
                summary.getMax() - summary.getMin(),
                lessThanOrEqualTo(1)
            );

            totalOutgoingMoves.keySet().removeIf(nodeId -> isReconciled(allocation.routingNodes().node(nodeId), balance));
            clusterState = ClusterState.builder(clusterState)
                .routingTable(RoutingTable.of(allocation.routingTable().version(), allocation.routingNodes()))
                .build();
        }
    }

    public void testShouldLogOnTooManyUndesiredAllocations() {

        var indexMetadata = IndexMetadata.builder("index-1").settings(indexSettings(Version.CURRENT, 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("data-node-1")).add(newNode("data-node-2")))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(index).addShard(newShardRouting(shardId, "data-node-2", true, STARTED)))
            )
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("data-node-1"), 1, 0, 0)));

        var threadPool = mock(ThreadPool.class);
        when(threadPool.relativeTimeInMillis()).thenReturn(1L).thenReturn(2L);

        var reconciler = new DesiredBalanceReconciler(createBuiltInClusterSettings(), threadPool);

        assertThatLogger(
            () -> reconciler.reconcile(balance, createRoutingAllocationFrom(clusterState)),
            DesiredBalanceReconciler.class,
            new MockLogAppender.SeenEventExpectation(
                "Should log first too many shards on undesired locations",
                DesiredBalanceReconciler.class.getCanonicalName(),
                Level.WARN,
                "[100.0%] of assigned shards (1/1) are not on their desired nodes, which exceeds the warn threshold of [10.0%]"
            )
        );
        assertThatLogger(
            () -> reconciler.reconcile(balance, createRoutingAllocationFrom(clusterState)),
            DesiredBalanceReconciler.class,
            new MockLogAppender.UnseenEventExpectation(
                "Should not log immediate second too many shards on undesired locations",
                DesiredBalanceReconciler.class.getCanonicalName(),
                Level.WARN,
                "[100.0%] of assigned shards (1/1) are not on their desired nodes, which exceeds the warn threshold of [10.0%]"
            )
        );
    }

    private static void reconcile(RoutingAllocation routingAllocation, DesiredBalance desiredBalance) {
        new DesiredBalanceReconciler(createBuiltInClusterSettings(), mock(ThreadPool.class)).reconcile(desiredBalance, routingAllocation);
    }

    private static boolean isReconciled(RoutingNode node, DesiredBalance balance) {
        for (ShardRouting shardRouting : node) {
            if (balance.assignments().get(shardRouting.shardId()).nodeIds().contains(node.nodeId()) == false) {
                return false;
            }
        }
        return true;
    }

    private static AllocationService createTestAllocationService(
        Consumer<RoutingAllocation> allocationConsumer,
        AllocationDecider... allocationDeciders
    ) {
        return createTestAllocationService(
            allocationConsumer,
            () -> ClusterInfo.EMPTY,
            () -> SnapshotShardSizeInfo.EMPTY,
            allocationDeciders
        );
    }

    private static RoutingAllocation createRoutingAllocationFrom(ClusterState clusterState, AllocationDecider... deciders) {
        return new RoutingAllocation(
            new AllocationDeciders(List.of(deciders)),
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
    }

    private static AllocationService createTestAllocationService(
        Consumer<RoutingAllocation> allocationConsumer,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        AllocationDecider... allocationDeciders
    ) {
        final var allocationService = new AllocationService(new AllocationDeciders(List.of(allocationDeciders)), new ShardsAllocator() {
            @Override
            public void allocate(RoutingAllocation allocation) {
                allocationConsumer.accept(allocation);
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new AssertionError("should not be called");
            }
        }, clusterInfoService, snapshotsInfoService, TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        allocationService.setExistingShardsAllocators(Map.of(GatewayAllocator.ALLOCATOR_NAME, new NoOpExistingShardsAllocator()));
        return allocationService;
    }

    private static class NoOpExistingShardsAllocator implements ExistingShardsAllocator {
        @Override
        public void beforeAllocation(RoutingAllocation allocation) {}

        @Override
        public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {}

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {}

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(
            ShardRouting unassignedShard,
            RoutingAllocation routingAllocation
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void cleanCaches() {}

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {}

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {}

        @Override
        public int getNumberOfInFlightFetches() {
            return 0;
        }
    }

    private static DesiredBalance desiredBalance(ClusterState clusterState, BiPredicate<ShardId, String> isDesiredPredicate) {
        return new DesiredBalance(
            1,
            StreamSupport.stream(clusterState.routingTable().spliterator(), false)
                .flatMap(indexRoutingTable -> IntStream.range(0, indexRoutingTable.size()).mapToObj(indexRoutingTable::shard))
                .collect(
                    Collectors.toMap(
                        IndexShardRoutingTable::shardId,
                        indexShardRoutingTable -> clusterState.nodes()
                            .stream()
                            .map(DiscoveryNode::getId)
                            .filter(nodeId -> isDesiredPredicate.test(indexShardRoutingTable.shardId(), nodeId))
                            .collect(Collectors.collectingAndThen(Collectors.toSet(), set -> new ShardAssignment(set, set.size(), 0, 0)))
                    )
                )
        );
    }

    private static DiscoveryNodes discoveryNodes(int nodeCount) {
        final var discoveryNodes = DiscoveryNodes.builder();
        for (var i = 0; i < nodeCount; i++) {
            discoveryNodes.add(newNode("node-" + i, "node-" + i, Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)));
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");
        return discoveryNodes.build();
    }

    @BeforeClass
    public static void populateCreationDates() {
        creationDates = randomArray(5, 5, Long[]::new, ESTestCase::randomNonNegativeLong);
    }

    // use relatively small set of creation dates so that they will occasionally be equal
    private static Long[] creationDates;

    private static IndexMetadata randomPriorityIndex(String name, int numberOfShards, int numberOfReplicas) {
        return IndexMetadata.builder(name)
            .settings(
                indexSettings(Version.CURRENT, numberOfShards, numberOfReplicas).put(
                    IndexMetadata.INDEX_PRIORITY_SETTING.getKey(),
                    between(1, 5)
                ).put(IndexMetadata.SETTING_CREATION_DATE, randomFrom(creationDates))
            )
            .system(randomBoolean())
            .build();
    }

    private static Settings throttleSettings() {
        return Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 1000)
            .build();
    }
}
