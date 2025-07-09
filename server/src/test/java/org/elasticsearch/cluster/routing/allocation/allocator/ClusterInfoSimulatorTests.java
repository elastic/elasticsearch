/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfo.NodeAndPath;
import org.elasticsearch.cluster.ClusterInfo.ReservedSpace;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoSimulatorTests extends ESAllocationTestCase {

    public void testInitializeNewPrimary() {

        var newPrimary = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), "node-0", true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder(1000, 1000))
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                    .build()
            )
        );
    }

    public void testInitializePreviouslyExistingPrimary() {

        var existingPrimary = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), "node-0", true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(RecoverySource.ExistingStoreRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder(1000, 900))
            .withShard(existingPrimary, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(existingPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 900))
                    .withShard(existingPrimary, 100)
                    .build()
            )
        );
    }

    public void testInitializeNewReplica() {

        var existingPrimary = newShardRouting(new ShardId("my-index", "_na_", 0), "node-0", true, STARTED);
        var newReplica = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), "node-1", false, INITIALIZING).withRecoverySource(
            RecoverySource.PeerRecoverySource.INSTANCE
        ).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder(1000, 900))
            .withNode("node-1", new DiskUsageBuilder(1000, 1000))
            .withShard(existingPrimary, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
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

    public void testInitializeNewReplicaWithReservedSpace() {

        var recoveredSize = 70;
        var remainingSize = 30;
        var totalShardSize = recoveredSize + remainingSize;

        var indexMetadata = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        var existingPrimary = newShardRouting(new ShardId(indexMetadata.getIndex(), 0), "node-0", true, STARTED);
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        var newReplica = shardRoutingBuilder(shardId, "node-1", false, INITIALIZING).withRecoverySource(
            RecoverySource.PeerRecoverySource.INSTANCE
        ).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
            .withNode("node-1", new DiskUsageBuilder("/data", 1000, 1000 - recoveredSize))
            .withShard(existingPrimary, totalShardSize)
            .withReservedSpace("node-1", "/data", remainingSize, newReplica.shardId())
            .build();

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(existingPrimary).addShard(newReplica))
            )
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
                    .withNode("node-1", new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
                    .withShard(existingPrimary, totalShardSize)
                    .withShard(newReplica, totalShardSize)
                    .build()
            )
        );
    }

    public void testRelocateShard() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(fromNodeId, new DiskUsageBuilder(1000, 900))
            .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
            .withShard(shard, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
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

    public void testRelocateShardWithMultipleDataPath() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(fromNodeId, new DiskUsageBuilder("/data-1", 1000, 500), new DiskUsageBuilder("/data-2", 1000, 750))
            .withNode(toNodeId, new DiskUsageBuilder("/data-1", 1000, 750), new DiskUsageBuilder("/data-2", 1000, 900))
            .withShard(shard, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
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

    public void testInitializeShardFromSnapshot() {

        var shardSize = 100;
        var indexSettings = indexSettings(IndexVersion.current(), 1, 0);
        if (randomBoolean()) {
            indexSettings.put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE);
        }

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");
        var shard = shardRoutingBuilder(
            new ShardId(state.metadata().getProject().index("my-index").getIndex(), 0),
            "node-0",
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder(1000, 1000))
            .withNode("node-1", new DiskUsageBuilder(1000, 1000))
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder() //
            .withShard(snapshot, indexId, shard.shardId(), shardSize)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, snapshotShardSizeInfo);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000 - shardSize))
                    .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                    .withShard(shard, shardSize)
                    .build()
            )
        );
    }

    public void testInitializeShardFromPartialSearchableSnapshot() {

        var shardSize = 100;
        var indexSettings = indexSettings(IndexVersion.current(), 1, 0) //
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
            .put(SNAPSHOT_PARTIAL_SETTING.getKey(), true)
            .put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");
        var shard = shardRoutingBuilder(
            new ShardId(state.metadata().getProject().index("my-index").getIndex(), 0),
            "node-0",
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder(1000, 1000))
            .withNode("node-1", new DiskUsageBuilder(1000, 1000))
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder() //
            .withShard(snapshot, indexId, shard.shardId(), shardSize)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, snapshotShardSizeInfo);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000))
                    .withNode("node-1", new DiskUsageBuilder(1000, 1000))
                    .withShard(shard, 0) // partial searchable snapshot always reports 0 size
                    .build()
            )
        );
    }

    public void testRelocatePartialSearchableSnapshotShard() {

        var shardSize = 100;
        var indexSettings = indexSettings(IndexVersion.current(), 1, 0) //
            .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
            .put(SNAPSHOT_PARTIAL_SETTING.getKey(), true)
            .put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(fromNodeId, new DiskUsageBuilder(1000, 1000))
            .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
            .withShard(shard, 0)
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder() //
            .withShard(snapshot, indexId, shard.shardId(), shardSize)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, snapshotShardSizeInfo);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(fromNodeId, new DiskUsageBuilder(1000, 1000))
                    .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
                    .withShard(shard, 0)  // partial searchable snapshot always reports 0 size
                    .build()
            )
        );
    }

    public void testInitializeShardFromClone() {

        var sourceShardSize = randomLongBetween(100, 1000);
        var source = newShardRouting(new ShardId("source", "_na_", 0), randomIdentifier(), true, ShardRoutingState.STARTED);
        var target = shardRoutingBuilder(new ShardId("target", "_na_", 0), randomIdentifier(), true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(RecoverySource.LocalShardsRecoverySource.INSTANCE)
            .build();

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder("source").settings(indexSettings(IndexVersion.current(), 1, 0)))
                    .put(
                        IndexMetadata.builder("target")
                            .settings(
                                indexSettings(IndexVersion.current(), 1, 0) //
                                    .put(INDEX_RESIZE_SOURCE_NAME_KEY, "source") //
                                    .put(INDEX_RESIZE_SOURCE_UUID_KEY, "_na_")
                            )
                    )
            )
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(source.index()).addShard(source))
                    .add(IndexRoutingTable.builder(target.index()).addShard(target))
            )
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder().withNode("node-0", new DiskUsageBuilder(1000, 1000 - sourceShardSize))
            .withShard(source, sourceShardSize)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(target);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode("node-0", new DiskUsageBuilder(1000, 1000 - sourceShardSize))
                    .withShard(source, sourceShardSize)
                    .withShard(target, sourceShardSize)
                    .build()
            )
        );
    }

    public void testDiskUsageSimulationWithSingleDataPathAndDiskThresholdDecider() {

        var discoveryNodesBuilder = DiscoveryNodes.builder().add(newNode("node-0")).add(newNode("node-1")).add(newNode("node-2"));

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        var shard1 = newShardRouting("index-1", 0, "node-0", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard1);

        var shard2 = shardRoutingBuilder(new ShardId("index-2", "_na_", 0), "node-0", true, INITIALIZING).withRelocatingNodeId("node-1")
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();
        addIndex(metadataBuilder, routingTableBuilder, shard2);

        var shard3 = newShardRouting("index-3", 0, "node-1", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard3);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 500))
            .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 300))
            .withShard(shard1, 500)
            .withShard(shard2, 400)
            .withShard(shard3, 300)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
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

        var decider = new DiskThresholdDecider(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(Settings.EMPTY));
        allocation = createRoutingAllocation(state, simulator.getClusterInfo(), SnapshotShardSizeInfo.EMPTY, decider);

        assertThat(
            "Should keep index-1 on node-0",
            decider.canRemain(state.metadata().getProject().index("index-1"), shard1, allocation.routingNodes().node("node-0"), allocation)
                .type(),
            equalTo(Decision.Type.YES)
        );
        assertThat(
            "Should keep index-2 on node-0",
            decider.canRemain(state.metadata().getProject().index("index-2"), shard2, allocation.routingNodes().node("node-0"), allocation)
                .type(),
            equalTo(Decision.Type.YES)
        );
        assertThat(
            "Should not allocate index-3 on node-0 (not enough space)",
            decider.canAllocate(shard3, allocation.routingNodes().node("node-0"), allocation).type(),
            equalTo(Decision.Type.NO)
        );
    }

    public void testDiskUsageSimulationWithMultipleDataPathAndDiskThresholdDecider() {

        var discoveryNodesBuilder = DiscoveryNodes.builder().add(newNode("node-0")).add(newNode("node-1")).add(newNode("node-2"));

        var metadataBuilder = Metadata.builder();
        var routingTableBuilder = RoutingTable.builder();

        var shard1 = newShardRouting("index-1", 0, "node-0", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard1);

        var shard2 = shardRoutingBuilder(new ShardId("index-2", "_na_", 0), "node-0", true, INITIALIZING).withRelocatingNodeId("node-1")
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();
        addIndex(metadataBuilder, routingTableBuilder, shard2);

        var shard3 = newShardRouting("index-3", 0, "node-1", null, true, STARTED);
        addIndex(metadataBuilder, routingTableBuilder, shard3);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode("node-0", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 500))
            .withNode("node-1", new DiskUsageBuilder("/data-1", 1000, 100), new DiskUsageBuilder("/data-2", 1000, 300))
            .withShard(shard1, 500)
            .withShard(shard2, 400)
            .withShard(shard3, 300)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
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

        var decider = new DiskThresholdDecider(Settings.EMPTY, ClusterSettings.createBuiltInClusterSettings(Settings.EMPTY));
        allocation = createRoutingAllocation(state, simulator.getClusterInfo(), SnapshotShardSizeInfo.EMPTY, decider);

        assertThat(
            "Should keep index-1 on node-0",
            decider.canRemain(state.metadata().getProject().index("index-1"), shard1, allocation.routingNodes().node("node-0"), allocation)
                .type(),
            equalTo(Decision.Type.YES)
        );

        assertThat(
            "Should keep index-2 on node-0",
            decider.canRemain(state.metadata().getProject().index("index-2"), shard2, allocation.routingNodes().node("node-0"), allocation)
                .type(),
            equalTo(Decision.Type.YES)
        );

        assertThat(
            "Should not allocate index-3 on node-0 (not enough space)",
            decider.canAllocate(shard3, allocation.routingNodes().node("node-0"), allocation).type(),
            equalTo(Decision.Type.NO)
        );
    }

    private static void addIndex(Metadata.Builder metadataBuilder, RoutingTable.Builder routingTableBuilder, ShardRouting shardRouting) {
        var name = shardRouting.getIndexName();
        metadataBuilder.put(IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)));
        routingTableBuilder.add(IndexRoutingTable.builder(metadataBuilder.get(name).getIndex()).addShard(shardRouting));
    }

    private static RoutingAllocation createRoutingAllocation(
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo,
        AllocationDecider... deciders
    ) {
        return new RoutingAllocation(new AllocationDeciders(List.of(deciders)), state, clusterInfo, snapshotShardSizeInfo, 0);
    }

    private static class SnapshotShardSizeInfoTestBuilder {

        private final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes = new HashMap<>();

        public SnapshotShardSizeInfoTestBuilder withShard(Snapshot snapshot, IndexId indexId, ShardId shardId, long size) {
            snapshotShardSizes.put(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shardId), size);
            return this;
        }

        public SnapshotShardSizeInfo build() {
            return new SnapshotShardSizeInfo(snapshotShardSizes);
        }
    }

    private static class ClusterInfoTestBuilder {

        private final Map<String, DiskUsage> leastAvailableSpaceUsage = new HashMap<>();
        private final Map<String, DiskUsage> mostAvailableSpaceUsage = new HashMap<>();
        private final Map<String, Long> shardSizes = new HashMap<>();
        private final Map<NodeAndPath, ReservedSpace> reservedSpace = new HashMap<>();

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
            shardSizes.put(shardIdentifierFromRouting(shard), size);
            return this;
        }

        public ClusterInfoTestBuilder withReservedSpace(String nodeId, String path, long size, ShardId... shardIds) {
            reservedSpace.put(new NodeAndPath(nodeId, nodeId + path), new ReservedSpace(size, Set.of(shardIds)));
            return this;
        }

        public ClusterInfo build() {
            return new ClusterInfo(
                leastAvailableSpaceUsage,
                mostAvailableSpaceUsage,
                shardSizes,
                Map.of(),
                Map.of(),
                reservedSpace,
                Map.of()
            );
        }
    }

    private record DiskUsageBuilder(String path, long total, long free) {

        private DiskUsageBuilder(long total, long free) {
            this("/data", total, free);
        }

        public DiskUsage toDiskUsage(String nodeId) {
            return new DiskUsage(nodeId, nodeId, nodeId + path, total, free);
        }
    }
}
