/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfo.NodeAndPath;
import org.elasticsearch.cluster.ClusterInfo.ReservedSpace;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.NODE_LEFT;
import static org.elasticsearch.cluster.routing.UnassignedInfo.Reason.REINITIALIZED;
import static org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClusterInfoSimulatorTests extends ESAllocationTestCase {

    public void testInitializeNewPrimary() {

        var nodeId = "node-0";

        var newPrimary = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), nodeId, true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(RecoverySource.EmptyStoreRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId, new DiskUsageBuilder(1000, 1000))
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId)).build())
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(newPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId, new DiskUsageBuilder(1000, 1000))
                    .build()
            )
        );
    }

    public void testInitializePreviouslyExistingPrimary() {

        var nodeId = "node-0";

        var existingPrimary = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), nodeId, true, ShardRoutingState.INITIALIZING)
            .withRecoverySource(RecoverySource.ExistingStoreRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId, new DiskUsageBuilder(1000, 900))
            .withShard(existingPrimary, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId)).build())
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(existingPrimary);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId, new DiskUsageBuilder(1000, 900))
                    .withShard(existingPrimary, 100)
                    .build()
            )
        );
    }

    public void testInitializeNewReplica() {

        var nodeId0 = "node-0";
        var nodeId1 = "node-1";

        var existingPrimary = newShardRouting(new ShardId("my-index", "_na_", 0), nodeId0, true, STARTED);
        var newReplica = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), nodeId1, false, INITIALIZING).withRecoverySource(
            RecoverySource.PeerRecoverySource.INSTANCE
        ).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId0, new DiskUsageBuilder(1000, 900))
            .withNode(nodeId1, new DiskUsageBuilder(1000, 1000))
            .withShard(existingPrimary, 100)
            .build();

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId0)).add(DiscoveryNodeUtils.create(nodeId1)).build())
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId0, new DiskUsageBuilder(1000, 900))
                    .withNode(nodeId1, new DiskUsageBuilder(1000, 900))
                    .withShard(existingPrimary, 100)
                    .withShard(newReplica, 100)
                    .build()
            )
        );
    }

    public void testInitializeNewReplicaWithReservedSpace() {

        var nodeId0 = "node-0";
        var nodeId1 = "node-1";

        var recoveredSize = 70;
        var remainingSize = 30;
        var totalShardSize = recoveredSize + remainingSize;

        var indexMetadata = IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        var existingPrimary = newShardRouting(new ShardId(indexMetadata.getIndex(), 0), nodeId0, true, STARTED);
        ShardId shardId = new ShardId(indexMetadata.getIndex(), 0);
        var newReplica = shardRoutingBuilder(shardId, nodeId1, false, INITIALIZING).withRecoverySource(
            RecoverySource.PeerRecoverySource.INSTANCE
        ).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId0, new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
            .withNode(nodeId1, new DiskUsageBuilder("/data", 1000, 1000 - recoveredSize))
            .withShard(existingPrimary, totalShardSize)
            .withReservedSpace(nodeId1, "/data", remainingSize, newReplica.shardId())
            .build();

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(existingPrimary).addShard(newReplica))
            )
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId0)).add(DiscoveryNodeUtils.create(nodeId1)).build())
            .build();
        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(newReplica);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId0, new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
                    .withNode(nodeId1, new DiskUsageBuilder("/data", 1000, 1000 - totalShardSize))
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
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(fromNodeId)).add(DiscoveryNodeUtils.create(toNodeId)).build())
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
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(fromNodeId)).add(DiscoveryNodeUtils.create(toNodeId)).build())
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

        var nodeId0 = "node-0";
        var nodeId1 = "node-1";

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
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId0)).add(DiscoveryNodeUtils.create(nodeId1)).build())
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");
        var shard = shardRoutingBuilder(
            new ShardId(state.metadata().getProject().index("my-index").getIndex(), 0),
            nodeId0,
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId0, new DiskUsageBuilder(1000, 1000))
            .withNode(nodeId1, new DiskUsageBuilder(1000, 1000))
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder().withShard(snapshot, indexId, shard.shardId(), shardSize).build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, snapshotShardSizeInfo);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(shard);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId0, new DiskUsageBuilder(1000, 1000 - shardSize))
                    .withNode(nodeId1, new DiskUsageBuilder(1000, 1000))
                    .withShard(shard, shardSize)
                    .build()
            )
        );
    }

    public void testInitializeShardFromPartialSearchableSnapshot() {

        var nodeId0 = "node-0";
        var nodeId1 = "node-1";

        var shardSize = 100;
        var indexSettings = indexSettings(IndexVersion.current(), 1, 0).put(
            INDEX_STORE_TYPE_SETTING.getKey(),
            SEARCHABLE_SNAPSHOT_STORE_TYPE
        ).put(SNAPSHOT_PARTIAL_SETTING.getKey(), true).put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId0)).add(DiscoveryNodeUtils.create(nodeId1)).build())
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");
        var shard = shardRoutingBuilder(
            new ShardId(state.metadata().getProject().index("my-index").getIndex(), 0),
            nodeId0,
            true,
            ShardRoutingState.INITIALIZING
        ).withRecoverySource(new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)).build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId0, new DiskUsageBuilder(1000, 1000))
            .withNode(nodeId1, new DiskUsageBuilder(1000, 1000))
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder().withShard(snapshot, indexId, shard.shardId(), shardSize).build();

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

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shardSize = 100;
        var indexSettings = indexSettings(IndexVersion.current(), 1, 0).put(
            INDEX_STORE_TYPE_SETTING.getKey(),
            SEARCHABLE_SNAPSHOT_STORE_TYPE
        ).put(SNAPSHOT_PARTIAL_SETTING.getKey(), true).put(SETTING_IGNORE_DISK_WATERMARKS.getKey(), true);

        final IndexMetadata index = IndexMetadata.builder("my-index").settings(indexSettings).build();
        final RoutingTable projectRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(index)
            .build();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(projectRoutingTable)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(fromNodeId)).add(DiscoveryNodeUtils.create(toNodeId)).build())
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");

        var shard = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(fromNodeId, new DiskUsageBuilder(1000, 1000))
            .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
            .withShard(shard, 0)
            .build();
        var snapshotShardSizeInfo = new SnapshotShardSizeInfoTestBuilder().withShard(snapshot, indexId, shard.shardId(), shardSize).build();

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

        var nodeId0 = "node-0";

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
                                indexSettings(IndexVersion.current(), 1, 0).put(INDEX_RESIZE_SOURCE_NAME_KEY, "source")
                                    .put(INDEX_RESIZE_SOURCE_UUID_KEY, "_na_")
                            )
                    )
            )
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(source.index()).addShard(source))
                    .add(IndexRoutingTable.builder(target.index()).addShard(target))
            )
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(nodeId0)).build())
            .build();

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(nodeId0, new DiskUsageBuilder(1000, 1000 - sourceShardSize))
            .withShard(source, sourceShardSize)
            .build();

        var allocation = createRoutingAllocation(state, initialClusterInfo, SnapshotShardSizeInfo.EMPTY);
        var simulator = new ClusterInfoSimulator(allocation);
        simulator.simulateShardStarted(target);

        assertThat(
            simulator.getClusterInfo(),
            equalTo(
                new ClusterInfoTestBuilder() //
                    .withNode(nodeId0, new DiskUsageBuilder(1000, 1000 - sourceShardSize))
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

    public void testSimulateAlreadyStartedShard() {
        final var clusterInfoSimulator = mock(ClusterInfoSimulator.class);
        doCallRealMethod().when(clusterInfoSimulator).simulateAlreadyStartedShard(any(), any());

        final ShardRouting startedShard = ShardRouting.newUnassigned(
            new ShardId(randomIdentifier(), randomUUID(), between(0, 5)),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(REINITIALIZED, "simulation"),
            ShardRouting.Role.DEFAULT
        ).initialize(randomIdentifier(), null, randomLongBetween(100, 999)).moveToStarted(randomLongBetween(100, 999));

        // New shard without relocation
        {
            final ArgumentCaptor<ShardRouting> shardCaptor = ArgumentCaptor.forClass(ShardRouting.class);
            clusterInfoSimulator.simulateAlreadyStartedShard(startedShard, null);
            verify(clusterInfoSimulator).simulateShardStarted(shardCaptor.capture());
            final var captureShard = shardCaptor.getValue();
            assertTrue(captureShard.initializing());
            assertNull(captureShard.relocatingNodeId());
            assertThat(captureShard.shardId(), equalTo(startedShard.shardId()));
            assertThat(captureShard.currentNodeId(), equalTo(startedShard.currentNodeId()));
            assertThat(captureShard.getExpectedShardSize(), equalTo(startedShard.getExpectedShardSize()));
        }

        // Relocation
        {
            Mockito.clearInvocations(clusterInfoSimulator);
            final ArgumentCaptor<ShardRouting> shardCaptor = ArgumentCaptor.forClass(ShardRouting.class);
            final String sourceNodeId = randomIdentifier();
            clusterInfoSimulator.simulateAlreadyStartedShard(startedShard, sourceNodeId);
            verify(clusterInfoSimulator).simulateShardStarted(shardCaptor.capture());
            final var captureShard = shardCaptor.getValue();
            assertTrue(captureShard.initializing());
            assertThat(captureShard.relocatingNodeId(), equalTo(sourceNodeId));
            assertThat(captureShard.shardId(), equalTo(startedShard.shardId()));
            assertThat(captureShard.currentNodeId(), equalTo(startedShard.currentNodeId()));
            assertThat(captureShard.getExpectedShardSize(), equalTo(startedShard.getExpectedShardSize()));
        }

    }

    /**
     * Tests relocating and initializing shards with heap usage estimates.
     */
    public void testHeapUsageSimulationWithEstimates() {
        var harness = setupHeapUsageTestHarness();
        var shardRouting1 = harness.shardRouting1; // Need to update these reference, harness doesn't allow it (as a record type.
        var shardRouting2 = harness.shardRouting2;

        /** Set up ClusterInfo */

        final long totalBytes = 500;
        final long estimatedBytesUsed = 250;
        final long shardHeapUsage = 50;
        final long indexHeapUsage = 10;

        final Map<String, EstimatedHeapUsage> estimatedHeapUsages = new HashMap<>();
        estimatedHeapUsages.put(harness.nodeId1, new EstimatedHeapUsage(harness.nodeId1, totalBytes, estimatedBytesUsed));
        estimatedHeapUsages.put(harness.nodeId2, new EstimatedHeapUsage(harness.nodeId2, totalBytes, estimatedBytesUsed));
        final Map<ShardId, ShardAndIndexHeapUsage> estimatedShardHeapUsages = new HashMap<>();
        estimatedShardHeapUsages.put(shardRouting1.shardId(), new ShardAndIndexHeapUsage(shardHeapUsage, indexHeapUsage));
        estimatedShardHeapUsages.put(shardRouting2.shardId(), new ShardAndIndexHeapUsage(shardHeapUsage, indexHeapUsage));

        ClusterInfo clusterInfo = ClusterInfo.builder()
            .estimatedHeapUsages(estimatedHeapUsages)
            .estimatedShardHeapUsages(estimatedShardHeapUsages)
            .build();

        /** Set up the Simulator */

        final var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            harness.clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        ).mutableCloneForSimulation();

        final var simulator = new ClusterInfoSimulator(allocation);

        /** Initially, the estimated heap usage should be what was initialized in the ClusterInfo above. */

        var nodeHeapUsages = simulator.getEstimatedHeapUsages();
        assertThat(nodeHeapUsages.get(harness.nodeId1).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));
        assertThat(nodeHeapUsages.get(harness.nodeId2).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));

        var sourceNodeId = shardRouting1.currentNodeId();
        var targetNodeId = sourceNodeId == harness.nodeId1 ? harness.nodeId2 : harness.nodeId1;

        /** Relocate a shard, check that the source loses and the target gains heap usage. */
        {
            var relocationShards = allocation.routingNodes()
                .relocateShard(
                    shardRouting1,
                    targetNodeId,
                    allocation.clusterInfo().getShardSize(shardRouting1, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    "relocating shard in test",
                    allocation.changes()
                );
            boolean indexRemovedFromSource = allocation.routingNodes()
                .node(relocationShards.v1().currentNodeId())
                .numberOfOwningShardsForIndex(shardRouting1.index()) == 0;
            boolean indexAddedToTarget = allocation.routingNodes()
                .node(relocationShards.v2().currentNodeId())
                .numberOfOwningShardsForIndex(shardRouting1.index()) == 1;
            assertThat(
                "Expected all shards to be assigned to one of the two nodes, since there are no replicas",
                indexRemovedFromSource,
                equalTo(false)
            );
            assertThat(
                "Expected one of the nodes to have no shards, since all primary shards are assigned to a single node",
                indexAddedToTarget,
                equalTo(true)
            );

            simulator.simulateShardStarted(relocationShards.v2());

            assertThat(
                "Expected the original node heap usage, "
                    + estimatedBytesUsed
                    + ", to have decreased by the shard's heap usage, "
                    + shardHeapUsage
                    + ", and though not by its index heap usage, "
                    + indexHeapUsage
                    + "; new node heap usage: "
                    + nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(),
                nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(),
                equalTo(estimatedBytesUsed - shardHeapUsage - (indexRemovedFromSource ? indexHeapUsage : 0))
            );
            assertThat(
                "Expected the original node heap usage, "
                    + estimatedBytesUsed
                    + ", to have increased by the shard's heap usage, "
                    + shardHeapUsage
                    + ", and its index heap usage, "
                    + indexHeapUsage
                    + "; new node heap usage: "
                    + nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(),
                nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(),
                equalTo(estimatedBytesUsed + shardHeapUsage + (indexAddedToTarget ? indexHeapUsage : 0))
            );

            // Want to continue testing with this shard later, so start it.
            shardRouting1 = allocation.routingNodes().startShard(relocationShards.v2(), allocation.changes(), 0);
        }

        /** Relocate the second index shard from the source node. This should remove the index heap usage from the source node. */
        {
            var sourceNodeHeapBeforeRelocation = nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes();
            var targetNodeHeapBeforeRelocation = nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes();

            var relocationShards = allocation.routingNodes()
                .relocateShard(
                    shardRouting2,
                    targetNodeId,
                    allocation.clusterInfo().getShardSize(shardRouting2, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    "relocating shard in test",
                    allocation.changes()
                );
            boolean indexRemovedFromSource = allocation.routingNodes()
                .node(relocationShards.v1().currentNodeId())
                .numberOfOwningShardsForIndex(shardRouting1.index()) == 0;
            boolean indexAddedToTarget = allocation.routingNodes()
                .node(relocationShards.v2().currentNodeId())
                .numberOfOwningShardsForIndex(shardRouting1.index()) == 1;
            assertThat("Expected all the index shards to be moved off the original node", indexRemovedFromSource, equalTo(true));
            assertThat("Expected all the index shards to be assigned to the other node now", indexAddedToTarget, equalTo(false));

            simulator.simulateShardStarted(relocationShards.v2());

            // Now, the estimated heap usage on nodeId2 should have increased with the new shard, while sourceNodeId remained the same.
            assertThat(
                "Expected the original node heap usage, "
                    + sourceNodeHeapBeforeRelocation
                    + ", to have decreased by the shard's heap usage, "
                    + shardHeapUsage
                    + ", and its index heap usage, "
                    + indexHeapUsage
                    + "; new node heap usage: "
                    + nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(),
                nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(),
                equalTo(sourceNodeHeapBeforeRelocation - shardHeapUsage - (indexRemovedFromSource ? indexHeapUsage : 0))
            );
            assertThat(
                "Expected the original node heap usage, "
                    + targetNodeHeapBeforeRelocation
                    + ", to have increased by the shard's heap usage, "
                    + shardHeapUsage
                    + ", and not its index heap usage, "
                    + indexHeapUsage
                    + "; new node heap usage: "
                    + nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(),
                nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(),
                equalTo(targetNodeHeapBeforeRelocation + shardHeapUsage + (indexAddedToTarget ? indexHeapUsage : 0))
            );

            shardRouting2 = allocation.routingNodes().startShard(relocationShards.v2(), allocation.changes(), 0);
        }

        /** Initialize a new shard */
        {
            assertThat("Shards should all be on the 'target' node", shardRouting1.currentNodeId(), equalTo(targetNodeId));
            var newTargetNodeId = sourceNodeId; // So we'll move the shard back to the 'source' node, making it the new target node.
            var targetNodeHeapBeforeAssignment = nodeHeapUsages.get(newTargetNodeId).getEstimatedUsageBytes();

            // Unassign the shard - note, this leaves the heap usage incorrect for the node that owned the shard, since the node
            // unrealistically didn't leave the cluster, and there is no shard removal simulation.
            allocation.routingNodes()
                .failShard(shardRouting1, new UnassignedInfo(NODE_LEFT, "unassigning shard for test"), allocation.changes());
            var unassignedIt = allocation.routingNodes().unassigned().iterator();
            assertTrue(unassignedIt.hasNext());
            var unassignedShardRouting = unassignedIt.next();
            assertFalse(unassignedIt.hasNext());
            // Initialize the shard on the other node.
            shardRouting1 = allocation.routingNodes()
                .initializeShard(unassignedShardRouting, newTargetNodeId, null, 0, allocation.changes());
            simulator.simulateShardStarted(shardRouting1);

            // Check that nodeId2 further increased the heap usage.
            assertThat(
                "Expect the original node heap usage, "
                    + estimatedBytesUsed
                    + ", to have increased by the shard's heap usage, "
                    + shardHeapUsage
                    + ", and its index heap usage, "
                    + indexHeapUsage,
                nodeHeapUsages.get(newTargetNodeId).getEstimatedUsageBytes(),
                equalTo(targetNodeHeapBeforeAssignment + shardHeapUsage + indexHeapUsage)
            );
        }
    }

    /**
     * Tests relocating and initializing shards with neither node nor shard level heap usage estimates.
     */
    public void testHeapUsageSimulationWithoutAnyEstimates() {
        var harness = setupHeapUsageTestHarness();
        var shardRouting1 = harness.shardRouting1; // Need to update these reference, harness doesn't allow it (as a record type.

        /** Set up ClusterInfo */

        ClusterInfo clusterInfo = ClusterInfo.builder().build();

        /** Set up the Simulator */

        final var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            harness.clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        ).mutableCloneForSimulation();

        final var simulator = new ClusterInfoSimulator(allocation);

        /** The estimated heap usage should be zero, as was initialized in the ClusterInfo above. */

        var nodeHeapUsages = simulator.getEstimatedHeapUsages();
        assertThat(nodeHeapUsages.get(harness.nodeId1).getEstimatedUsageBytes(), equalTo(0));
        assertThat(nodeHeapUsages.get(harness.nodeId2).getEstimatedUsageBytes(), equalTo(0));

        var sourceNodeId = shardRouting1.currentNodeId();
        var targetNodeId = sourceNodeId == harness.nodeId1 ? harness.nodeId2 : harness.nodeId1;

        /** Relocate a shard, heap usage should remain unchanged. */
        {
            var relocationShards = allocation.routingNodes()
                .relocateShard(
                    shardRouting1,
                    targetNodeId,
                    allocation.clusterInfo().getShardSize(shardRouting1, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    "relocating shard in test",
                    allocation.changes()
                );

            simulator.simulateShardStarted(relocationShards.v2());

            assertThat(nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(), equalTo(0));
            assertThat(nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(), equalTo(0));

            // Want to continue testing with this shard later, so start it.
            shardRouting1 = allocation.routingNodes().startShard(relocationShards.v2(), allocation.changes(), 0);
        }

        /** Initialize a new shard */
        {
            assertThat("Shards should all be on the 'target' node", shardRouting1.currentNodeId(), equalTo(targetNodeId));
            var newTargetNodeId = sourceNodeId; // So we'll move the shard back to the 'source' node, making it the new target node.

            // Unassign the shard -- unrealistic in the sense that the node is still part of the cluster state, but ok for testing purposes.
            allocation.routingNodes()
                .failShard(shardRouting1, new UnassignedInfo(NODE_LEFT, "unassigning shard for test"), allocation.changes());
            var unassignedIt = allocation.routingNodes().unassigned().iterator();
            assertTrue(unassignedIt.hasNext());
            var unassignedShardRouting = unassignedIt.next();
            assertFalse(unassignedIt.hasNext());
            // Initialize the shard on the other node.
            shardRouting1 = allocation.routingNodes()
                .initializeShard(unassignedShardRouting, newTargetNodeId, null, 0, allocation.changes());
            simulator.simulateShardStarted(shardRouting1);

            assertThat(nodeHeapUsages.get(newTargetNodeId).getEstimatedUsageBytes(), equalTo(0));
        }
    }

    /**
     * Tests relocating and initializing shards with node level heap usage estimates, but no shard level estimates.
     */
    public void testHeapUsageSimulationWithoutShardEstimates() {
        var harness = setupHeapUsageTestHarness();
        var shardRouting1 = harness.shardRouting1; // Need to update these reference, harness doesn't allow it (as a record type.

        /** Set up ClusterInfo */

        final long totalBytes = 500;
        final long estimatedBytesUsed = 250;

        final Map<String, EstimatedHeapUsage> estimatedHeapUsages = new HashMap<>();
        estimatedHeapUsages.put(harness.nodeId1, new EstimatedHeapUsage(harness.nodeId1, totalBytes, estimatedBytesUsed));
        estimatedHeapUsages.put(harness.nodeId2, new EstimatedHeapUsage(harness.nodeId2, totalBytes, estimatedBytesUsed));

        ClusterInfo clusterInfo = ClusterInfo.builder().estimatedHeapUsages(estimatedHeapUsages).build();

        /** Set up the Simulator */

        final var allocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            harness.clusterState,
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        ).mutableCloneForSimulation();

        final var simulator = new ClusterInfoSimulator(allocation);

        /** The estimated heap usage should be as initialized in the ClusterInfo above. */

        var nodeHeapUsages = simulator.getEstimatedHeapUsages();
        assertThat(nodeHeapUsages.get(harness.nodeId1).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));
        assertThat(nodeHeapUsages.get(harness.nodeId2).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));

        var sourceNodeId = shardRouting1.currentNodeId();
        var targetNodeId = sourceNodeId == harness.nodeId1 ? harness.nodeId2 : harness.nodeId1;

        /** Relocate a shard, heap usage should remain unchanged. */
        {
            var relocationShards = allocation.routingNodes()
                .relocateShard(
                    shardRouting1,
                    targetNodeId,
                    allocation.clusterInfo().getShardSize(shardRouting1, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE),
                    "relocating shard in test",
                    allocation.changes()
                );

            simulator.simulateShardStarted(relocationShards.v2());

            assertThat(nodeHeapUsages.get(sourceNodeId).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));
            assertThat(nodeHeapUsages.get(targetNodeId).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));

            // Want to continue testing with this shard later, so start it.
            shardRouting1 = allocation.routingNodes().startShard(relocationShards.v2(), allocation.changes(), 0);
        }

        /** Initialize a new shard */
        {
            assertThat("Shards should all be on the 'target' node", shardRouting1.currentNodeId(), equalTo(targetNodeId));
            var newTargetNodeId = sourceNodeId; // So we'll move the shard back to the 'source' node, making it the new target node.

            // Unassign the shard -- unrealistic in the sense that the node is still part of the cluster state, but ok for testing purposes.
            allocation.routingNodes()
                .failShard(shardRouting1, new UnassignedInfo(NODE_LEFT, "unassigning shard for test"), allocation.changes());
            var unassignedIt = allocation.routingNodes().unassigned().iterator();
            assertTrue(unassignedIt.hasNext());
            var unassignedShardRouting = unassignedIt.next();
            assertFalse(unassignedIt.hasNext());
            // Initialize the shard on the other node.
            shardRouting1 = allocation.routingNodes()
                .initializeShard(unassignedShardRouting, newTargetNodeId, null, 0, allocation.changes());
            simulator.simulateShardStarted(shardRouting1);

            assertThat(nodeHeapUsages.get(newTargetNodeId).getEstimatedUsageBytes(), equalTo(estimatedBytesUsed));
        }
    }

    private record HeapUsageTestHarness(
        ClusterState clusterState,
        String nodeId1,
        String nodeId2,
        ShardRouting shardRouting1,
        ShardRouting shardRouting2
    ) {}

    /** Set up ClusterState and references to state within it. */
    private HeapUsageTestHarness setupHeapUsageTestHarness() {
        final String indexName = "test-index-one";

        final ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(new String[] { indexName }, 2, 0);
        final var indexMetadata = clusterState.metadata().getProject(ProjectId.DEFAULT).index(indexName);
        assertThat(indexMetadata.getNumberOfShards(), equalTo(2));
        final var nodes = clusterState.nodes().getNodes();
        var shardRouting1 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).shard(0);
        var shardRouting2 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(1).shard(0);

        assertThat(nodes.size(), equalTo(2));
        final var nodesIt = nodes.entrySet().iterator();
        assertTrue(nodesIt.hasNext());
        final String nodeId1 = nodesIt.next().getKey();
        assertTrue(nodesIt.hasNext());
        final String nodeId2 = nodesIt.next().getKey();
        assertFalse(nodesIt.hasNext());

        return new HeapUsageTestHarness(clusterState, nodeId1, nodeId2, shardRouting1, shardRouting2);
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
            return ClusterInfo.builder()
                .leastAvailableSpaceUsage(leastAvailableSpaceUsage)
                .mostAvailableSpaceUsage(mostAvailableSpaceUsage)
                .shardSizes(shardSizes)
                .reservedSpace(reservedSpace)
                .build();
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
