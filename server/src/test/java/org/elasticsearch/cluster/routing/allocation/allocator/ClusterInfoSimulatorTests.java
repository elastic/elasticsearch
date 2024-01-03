/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoSimulatorTests extends ESTestCase {

    public void testInitializeNewPrimary() {

        var newPrimary = newShardRouting("index-1", 0, "node-0", true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                .withShard(newPrimary, 0)
                .build()
        );
        simulator.simulateShardStarted(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                    .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                    .withShard(newPrimary, 0)
                    .build()
            )
        );
    }

    public void testInitializeNewPrimaryWithKnownExpectedSize() {

        var newPrimary = newShardRouting("index-1", 0, null, true, UNASSIGNED).initialize("node-0", null, 100);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                .build()
        );
        simulator.simulateShardStarted(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 900))
                    .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                    .withShard(newPrimary, 100)
                    .build()
            )
        );
    }

    public void testInitializeNewReplica() {

        var existingPrimary = newShardRouting("index-1", 0, "node-0", true, STARTED);
        var newReplica = newShardRouting("index-1", 0, "node-1", false, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder(1000, 900))
                .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                .withShard(existingPrimary, 100)
                .withShard(newReplica, 0)
                .build()
        );
        simulator.simulateShardStarted(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 900))
                    .withNode("node-1", new DiskUsageBuilder(1000, 900))
                    .withShard(existingPrimary, 100)
                    .withShard(newReplica, 100)
                    .build()
            )
        );
    }

    public void testRelocateShard() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = newShardRouting("index-1", 0, toNodeId, fromNodeId, true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode(fromNodeId, new DiskUsageBuilder(1000, 900))
                .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
                .withShard(shard, 100)
                .build()
        );
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(fromNodeId, new DiskUsageBuilder(1000, 1000))
                    .withNode(toNodeId, new DiskUsageBuilder(1000, 900))
                    .withShard(shard, 100)
                    .build()
            )
        );
    }

    public void testRelocateShardWithMultipleDataPath1() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = newShardRouting("index-1", 0, toNodeId, fromNodeId, true, INITIALIZING);

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode(fromNodeId, new DiskUsageBuilder("/data-1", 1000, 500), new DiskUsageBuilder("/data-2", 1000, 750))
                .withNode(toNodeId, new DiskUsageBuilder("/data-1", 1000, 750), new DiskUsageBuilder("/data-2", 1000, 900))
                .withShard(shard, 100)
                .build()
        );
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(fromNodeId, new DiskUsageBuilder("/data-1", 1000, 500), new DiskUsageBuilder("/data-2", 1000, 850))
                    .withNode(toNodeId, new DiskUsageBuilder("/data-1", 1000, 750), new DiskUsageBuilder("/data-2", 1000, 800))
                    .withShard(shard, 100)
                    .build()
            )
        );
    }

    public void testDiskUsageSimulationWithSingleDataPathAndDiskThresholdDecider() {

        var discoveryNodesBuilder = DiscoveryNodes.builder()
            .add(createDiscoveryNode("node-0", DiscoveryNodeRole.roles()))
            .add(createDiscoveryNode("node-1", DiscoveryNodeRole.roles()))
            .add(createDiscoveryNode("node-2", DiscoveryNodeRole.roles()));

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        var shard1 = newShardRouting("index-1", 0, "node-0", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard1);

        var shard2 = newShardRouting("index-2", 0, "node-0", "node-1", true, INITIALIZING);
        addIndex(metadataBuilder, routingTableBuilder, shard2);

        var shard3 = newShardRouting("index-3", 0, "node-1", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard3);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 500))
                .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 300))
                .withShard(shard1, 500)
                .withShard(shard2, 400)
                .withShard(shard3, 300)
                .build()
        );

        simulator.simulateShardStarted(shard2);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 100))
                    .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 700))
                    .withShard(shard1, 500)
                    .withShard(shard2, 400)
                    .withShard(shard3, 300)
                    .build()
            )
        );

        var decider = new DiskThresholdDecider(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            clusterState,
            simulator.getClusterInfo(),
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
        var routingNodes = allocation.routingNodes();

        assertThat(
            "Should keep index-1 on node-0",
            decider.canRemain(clusterState.metadata().index("index-1"), shard1, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.YES)
        );
        assertThat(
            "Should keep index-2 on node-0",
            decider.canRemain(clusterState.metadata().index("index-2"), shard2, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.YES)
        );
        assertThat(
            "Should not allocate index-3 on node-0 (not enough space)",
            decider.canAllocate(shard3, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.NO)
        );
    }

    public void testDiskUsageSimulationWithMultipleDataPathAndDiskThresholdDecider() {

        var discoveryNodesBuilder = DiscoveryNodes.builder()
            .add(createDiscoveryNode("node-0", DiscoveryNodeRole.roles()))
            .add(createDiscoveryNode("node-1", DiscoveryNodeRole.roles()))
            .add(createDiscoveryNode("node-2", DiscoveryNodeRole.roles()));

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        var shard1 = newShardRouting("index-1", 0, "node-0", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard1);

        var shard2 = newShardRouting("index-2", 0, "node-0", "node-1", true, INITIALIZING);
        addIndex(metadataBuilder, routingTableBuilder, shard2);

        var shard3 = newShardRouting("index-3", 0, "node-1", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard3);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var simulator = new ClusterInfoSimulator(
            new ClusterInfoTestBuilder() //
                .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 500))
                .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 300))
                .withShard(shard1, 500)
                .withShard(shard2, 400)
                .withShard(shard3, 300)
                .build()
        );

        simulator.simulateShardStarted(shard2);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 100))
                    .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 700))
                    .withShard(shard1, 500)
                    .withShard(shard2, 400)
                    .withShard(shard3, 300)
                    .build()
            )
        );

        var decider = new DiskThresholdDecider(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of(decider)),
            clusterState,
            simulator.getClusterInfo(),
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
        var routingNodes = allocation.routingNodes();

        assertThat(
            "Should keep index-1 on node-0",
            decider.canRemain(clusterState.metadata().index("index-1"), shard1, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.YES)
        );

        assertThat(
            "Should keep index-2 on node-0",
            decider.canRemain(clusterState.metadata().index("index-2"), shard2, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.YES)
        );

        assertThat(
            "Should not allocate index-3 on node-0 (not enough space)",
            decider.canAllocate(shard3, routingNodes.node("node-0"), allocation).type(),
            equalTo(Decision.Type.NO)
        );
    }

    private static DiscoveryNode createDiscoveryNode(String id, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.builder(id).name(id).externalId(UUIDs.randomBase64UUID(random())).roles(roles).build();
    }

    private static void addIndex(Metadata.Builder metadataBuilder, RoutingTable.Builder routingTableBuilder, ShardRouting shardRouting) {
        var name = shardRouting.getIndexName();
        metadataBuilder.put(IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)));
        routingTableBuilder.add(IndexRoutingTable.builder(metadataBuilder.get(name).getIndex()).addShard(shardRouting));
    }

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> leastAvailableSpaceUsage = new HashMap<>();
        private final Map<String, DiskUsage> mostAvailableSpaceUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();

        public ClusterInfoTestBuilder withNode(String name, DiskUsageBuilder diskUsageBuilderBuilder) {
            leastAvailableSpaceUsage.put(name, diskUsageBuilderBuilder.toDiskUsage(name));
            mostAvailableSpaceUsage.put(name, diskUsageBuilderBuilder.toDiskUsage(name));
            return this;
        }

        public ClusterInfoTestBuilder withNode(String name, DiskUsageBuilder leastAvailableSpace, DiskUsageBuilder mostAvailableSpace) {
            leastAvailableSpaceUsage.put(name, leastAvailableSpace.toDiskUsage(name));
            mostAvailableSpaceUsage.put(name, mostAvailableSpace.toDiskUsage(name));
            return this;
        }

        public ClusterInfoTestBuilder withShard(ShardRouting shard, long size) {
            shardSizes.put(ClusterInfo.shardIdentifierFromRouting(shard), size);
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(leastAvailableSpaceUsage, mostAvailableSpaceUsage, shardSizes, Map.of(), Map.of(), Map.of());
        }
    }

    private record DiskUsageBuilder(String path, long total, long free) {

        private DiskUsageBuilder(long total, long free) {
            this("/data", total, free);
        }

        public DiskUsage toDiskUsage(String name) {
            return new DiskUsage(name, name, name + path, total, free);
        }
    }
}
