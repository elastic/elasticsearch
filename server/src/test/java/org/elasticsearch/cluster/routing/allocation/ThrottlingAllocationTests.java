/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.snapshots.SnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class ThrottlingAllocationTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ThrottlingAllocationTests.class);

    public void testPrimaryRecoveryThrottling() {

        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build(),
            gatewayAllocator,
            EmptyClusterInfoService.INSTANCE,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(6));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(9));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(10));
    }

    public void testReplicaAndPrimaryRecoveryThrottling() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.concurrent_source_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build(),
            gatewayAllocator,
            EmptyClusterInfoService.INSTANCE,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("with one node, do reroute, only 3 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));
    }

    public void testThrottleIncomingAndOutgoing() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 5)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 5)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 5)
            .put("cluster.routing.allocation.type", "balanced") // TODO fix for desired_balance
            .build();
        AllocationService strategy = createAllocationService(
            settings,
            gatewayAllocator,
            EmptyClusterInfoService.INSTANCE,
            snapshotsInfoService
        );
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );
        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(9).numberOfReplicas(0))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("with one node, do reroute, only 5 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(4));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 5);

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(4));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start another 2 nodes, 5 shards should be relocating - at most 5 are allowed per node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(4));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 3);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 2);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 5);

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the relocating shards, one more shard should relocate away from node1");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 0);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 1);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
    }

    public void testOutgoingThrottlesAllocation() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        TestSnapshotsInfoService snapshotsInfoService = new TestSnapshotsInfoService();
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1).build(),
            gatewayAllocator,
            EmptyClusterInfoService.INSTANCE,
            snapshotsInfoService
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitializeAllocations(metadata, gatewayAllocator, snapshotsInfoService);

        logger.info("with one node, do reroute, only 1 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(2));

        logger.info("start one more node, first non-primary should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start initializing non-primary");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);

        logger.info("start one more node, initializing second non-primary");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start one more node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("move started non-primary to new node");
        AllocationService.CommandsResult commandsResult = strategy.reroute(
            clusterState,
            new AllocationCommands(new MoveAllocationCommand("test", 0, "node2", "node4")),
            true,
            false,
            false,
            ActionListener.noop()
        );
        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.THROTTLE);
        boolean foundThrottledMessage = false;
        for (Decision decision : commandsResult.explanations().explanations().get(0).decisions().getDecisions()) {
            if (decision.label().equals(ThrottlingAllocationDecider.NAME)) {
                assertEquals(
                    "reached the limit of outgoing shard recoveries [1] on the node [node1] which holds the primary, "
                        + "cluster setting [cluster.routing.allocation.node_concurrent_outgoing_recoveries=1] "
                        + "(can also be set via [cluster.routing.allocation.node_concurrent_recoveries])",
                    decision.getExplanation()
                );
                assertEquals(Decision.Type.THROTTLE, decision.type());
                foundThrottledMessage = true;
            }
        }
        assertTrue(foundThrottledMessage);
        // even though it is throttled, move command still forces allocation

        clusterState = commandsResult.clusterState();
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 2);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), 0);
    }

    private ClusterState createRecoveryStateAndInitializeAllocations(
        final Metadata metadata,
        final TestGatewayAllocator gatewayAllocator,
        final TestSnapshotsInfoService snapshotsInfoService
    ) {
        DiscoveryNode node1 = newNode("node1");
        Metadata.Builder metadataBuilder = Metadata.builder(metadata);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", "randomId"));
        Set<String> snapshotIndices = new HashSet<>();
        String restoreUUID = UUIDs.randomBase64UUID();
        for (IndexMetadata im : metadata.indices().values()) {
            Index index = im.getIndex();
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(im);
            final int recoveryType = randomInt(5);
            if (recoveryType <= 4) {
                addInSyncAllocationIds(index, indexMetadataBuilder, gatewayAllocator, node1);
            }
            IndexMetadata indexMetadata = indexMetadataBuilder.build();
            metadataBuilder.put(indexMetadata, false);
            switch (recoveryType) {
                case 0 -> routingTableBuilder.addAsRecovery(indexMetadata);
                case 1 -> routingTableBuilder.addAsFromCloseToOpen(indexMetadata);
                case 2 -> routingTableBuilder.addAsFromDangling(indexMetadata);
                case 3 -> {
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsNewRestore(
                        indexMetadata,
                        new SnapshotRecoverySource(
                            restoreUUID,
                            snapshot,
                            IndexVersion.current(),
                            new IndexId(indexMetadata.getIndex().getName(), UUIDs.randomBase64UUID(random()))
                        ),
                        new HashSet<>()
                    );
                }
                case 4 -> {
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsRestore(
                        indexMetadata,
                        new SnapshotRecoverySource(
                            restoreUUID,
                            snapshot,
                            IndexVersion.current(),
                            new IndexId(indexMetadata.getIndex().getName(), UUIDs.randomBase64UUID(random()))
                        )
                    );
                }
                case 5 -> routingTableBuilder.addAsNew(indexMetadata);
                default -> throw new IndexOutOfBoundsException();
            }
        }

        final RoutingTable routingTable = routingTableBuilder.build();

        final Map<String, ClusterState.Custom> restores = new HashMap<>();
        if (snapshotIndices.isEmpty() == false) {
            // Some indices are restored from snapshot, the RestoreInProgress must be set accordingly
            Map<ShardId, RestoreInProgress.ShardRestoreStatus> restoreShards = new HashMap<>();
            for (ShardRouting shard : routingTable.allShardsIterator()) {
                if (shard.primary() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    final ShardId shardId = shard.shardId();
                    restoreShards.put(shardId, new RestoreInProgress.ShardRestoreStatus(node1.getId(), RestoreInProgress.State.INIT));
                    // Also set the snapshot shard size
                    final SnapshotRecoverySource recoverySource = (SnapshotRecoverySource) shard.recoverySource();
                    final long shardSize = randomNonNegativeLong();
                    snapshotsInfoService.addSnapshotShardSize(recoverySource.snapshot(), recoverySource.index(), shardId, shardSize);
                }
            }

            RestoreInProgress.Entry restore = new RestoreInProgress.Entry(
                restoreUUID,
                snapshot,
                RestoreInProgress.State.INIT,
                false,
                new ArrayList<>(snapshotIndices),
                restoreShards
            );
            restores.put(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build());
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1))
            .metadata(metadataBuilder.build())
            .routingTable(routingTable)
            .customs(restores)
            .build();
    }

    private void addInSyncAllocationIds(
        Index index,
        IndexMetadata.Builder indexMetadata,
        TestGatewayAllocator gatewayAllocator,
        DiscoveryNode node1
    ) {
        for (int shard = 0; shard < indexMetadata.numberOfShards(); shard++) {

            final boolean primary = randomBoolean();
            final ShardRouting unassigned = ShardRouting.newUnassigned(
                new ShardId(index, shard),
                primary,
                primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test"),
                ShardRouting.Role.DEFAULT
            );
            ShardRouting started = ShardRoutingHelper.moveToStarted(ShardRoutingHelper.initialize(unassigned, node1.getId()));
            indexMetadata.putInSyncAllocationIds(shard, Collections.singleton(started.allocationId().getId()));
            gatewayAllocator.addKnownAllocation(started);
        }
    }

    private static class TestSnapshotsInfoService implements SnapshotsInfoService {

        private volatile Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes = Map.of();

        synchronized void addSnapshotShardSize(Snapshot snapshot, IndexId index, ShardId shard, Long size) {
            final Map<InternalSnapshotsInfoService.SnapshotShard, Long> newSnapshotShardSizes = new HashMap<>(snapshotShardSizes);
            boolean added = newSnapshotShardSizes.put(new InternalSnapshotsInfoService.SnapshotShard(snapshot, index, shard), size) == null;
            assert added : "cannot add snapshot shard size twice";
            this.snapshotShardSizes = Collections.unmodifiableMap(newSnapshotShardSizes);
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizes() {
            return new SnapshotShardSizeInfo(snapshotShardSizes);
        }
    }
}
