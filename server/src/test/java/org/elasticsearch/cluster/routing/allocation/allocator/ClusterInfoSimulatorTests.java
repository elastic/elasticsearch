/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfo.NodeAndPath;
import org.elasticsearch.cluster.ClusterInfo.ReservedSpace;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
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
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterInfo.shardIdentifierFromRouting;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
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

    /**
     * Confirms {@link ClusterInfoSimulator} wires shard-move cache commitment deltas through to
     * {@link ClusterInfoSimulator#getClusterInfo()}.
     */
    public void testRelocateShardUpdatesCacheCommitments() {

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

        var shard = shardRoutingBuilder(new ShardId("my-index", "_na_", 0), toNodeId, true, INITIALIZING).withRelocatingNodeId(fromNodeId)
            .withRecoverySource(RecoverySource.PeerRecoverySource.INSTANCE)
            .build();

        final long cacheSize = randomLongBetween(100, 1000);
        final long fromNodeInitialBoostedCommitment = randomLongBetween(50, 300);
        final long fromNodeInitialUnboostedCommitment = randomLongBetween(50, 300);
        final long toNodeInitialBoostedCommitment = randomLongBetween(0, 100);
        final long toNodeInitialUnboostedCommitment = randomLongBetween(0, 100);
        final long boostedRequirement = randomLongBetween(1, 50);
        final long unboostedRequirement = randomLongBetween(1, 50);

        var initialClusterInfo = new ClusterInfoTestBuilder() //
            .withNode(fromNodeId, new DiskUsageBuilder(1000, 900))
            .withNode(toNodeId, new DiskUsageBuilder(1000, 1000))
            .withShard(shard, 100)
            .withNodeCacheSizeAndCommitments(fromNodeId, cacheSize, fromNodeInitialBoostedCommitment, fromNodeInitialUnboostedCommitment)
            .withNodeCacheSizeAndCommitments(toNodeId, cacheSize, toNodeInitialBoostedCommitment, toNodeInitialUnboostedCommitment)
            .withShardCacheRequirement(shard, boostedRequirement, unboostedRequirement)
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

        var updatedCommitments = simulator.getClusterInfo().getNodeCacheSizeAndCommitments();
        assertThat(
            updatedCommitments.get(fromNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    fromNodeInitialBoostedCommitment - boostedRequirement,
                    fromNodeInitialUnboostedCommitment - unboostedRequirement
                )
            )
        );
        assertThat(
            updatedCommitments.get(toNodeId),
            equalTo(
                new NodeCacheSizeAndCommitments(
                    cacheSize,
                    toNodeInitialBoostedCommitment + boostedRequirement,
                    toNodeInitialUnboostedCommitment + unboostedRequirement
                )
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

        var fromNodeId = "node-0";
        var toNodeId = "node-1";

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
                                indexSettings(IndexVersion.current(), 1, 0) //
                                    .put(INDEX_RESIZE_SOURCE_NAME_KEY, "source")
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
            ShardRouting.Role.DEFAULT,
            ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED
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
     * Node heap is derived from the routing table and the shard-heap snapshot. Totals stored on the node-heap snapshot are not a baseline:
     * that snapshot can disagree with where shards actually sit.
     */
    public void testNodeHeapIsRecomputedFromRoutingTable() {
        final String nodeA = "index-a";
        final String nodeB = "index-b";
        final String indexName = "test-index";
        final ClusterState clusterState = clusterStateWithRoles(
            indexName,
            Map.of(nodeA, Set.of(DiscoveryNodeRole.INDEX_ROLE), nodeB, Set.of(DiscoveryNodeRole.INDEX_ROLE)),
            nodeA,
            Map.of(0, nodeA, 1, nodeA)
        );
        final ShardId shard0 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).shardId();
        final ShardId shard1 = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(1).shardId();

        final long totalBytes = 10_000;
        final long nonShardHeapUsage = 40;
        // Deliberately inconsistent with the shards above, so a delta applied to this baseline would be wrong.
        final long staleTotal = 9_999;
        final long staleHosted = 8_888;
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeHeapMetrics(
                Map.of(
                    nodeA,
                    new NodeHeapMetrics(nodeA, totalBytes, new NodeHeapEstimates(staleTotal, staleHosted, nonShardHeapUsage)),
                    nodeB,
                    new NodeHeapMetrics(nodeB, totalBytes, new NodeHeapEstimates(staleTotal, staleHosted, nonShardHeapUsage))
                )
            )
            .estimatedShardHeapUsages(Map.of(shard0, new ShardAndIndexHeapUsage(50, 10, 5), shard1, new ShardAndIndexHeapUsage(80, 10, 20)))
            .build();

        final var allocation = TestRoutingAllocationFactory.forClusterState(clusterState)
            .clusterInfo(clusterInfo)
            .build()
            .mutableCloneForSimulation();
        final var simulator = new ClusterInfoSimulator(allocation);

        // Both shards are on node A. Index heap is counted once. Indexing-node totals use the larger postings sum, which is node A's 25.
        // hosted A = 50 + 80 + 10 = 140
        // total A = 40 + (50 - 5) + (80 - 20) + 10 + 25 = 180
        // node B hosts nothing, but is still charged the max postings: total B = 40 + 25 = 65
        var nodeHeapUsages = simulator.getClusterInfo().getNodeHeapMetrics();
        assertThat(nodeHeapUsages.get(nodeA).totalBytes(), equalTo(totalBytes));
        assertThat(nodeHeapUsages.get(nodeA).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(180, 140, nonShardHeapUsage)));
        assertThat(nodeHeapUsages.get(nodeB).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(65, 0, nonShardHeapUsage)));
        assertThat(simulator.getClusterInfo().getNodeHeapMetrics(), equalTo(nodeHeapUsages));

        final var relocation = allocation.routingNodes()
            .relocateShard(
                allocation.routingNodes().node(nodeA).getByShardId(shard1),
                nodeB,
                0L,
                "relocating shard in test",
                allocation.changes(),
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            );
        // The target copy is still initializing, so the shard still contributes on the source.
        assertThat(simulator.getClusterInfo().getNodeHeapMetrics().get(nodeA).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(140L));
        assertThat(simulator.getClusterInfo().getNodeHeapMetrics().get(nodeB).nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));

        allocation.routingNodes().startShard(relocation.v2(), allocation.changes(), 0L);
        // shard 0 stays on A (postings 5). shard 1 is on B (postings 20). Max postings is 20.
        // hosted A = 50 + 10 = 60; total A = 40 + (50 - 5) + 10 + 20 = 115
        // hosted B = 80 + 10 = 90; total B = 40 + (80 - 20) + 10 + 20 = 130
        nodeHeapUsages = simulator.getClusterInfo().getNodeHeapMetrics();
        assertThat(nodeHeapUsages.get(nodeA).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(115, 60, nonShardHeapUsage)));
        assertThat(nodeHeapUsages.get(nodeB).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(130, 90, nonShardHeapUsage)));

        final var secondRelocation = allocation.routingNodes()
            .relocateShard(
                allocation.routingNodes().node(nodeA).getByShardId(shard0),
                nodeB,
                0L,
                "relocating shard in test",
                allocation.changes(),
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            );
        allocation.routingNodes().startShard(secondRelocation.v2(), allocation.changes(), 0L);
        // Both shards are on B, so index heap is counted once there and the max postings is 25 again.
        nodeHeapUsages = simulator.getClusterInfo().getNodeHeapMetrics();
        assertThat(nodeHeapUsages.get(nodeA).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(65, 0, nonShardHeapUsage)));
        assertThat(nodeHeapUsages.get(nodeB).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(180, 140, nonShardHeapUsage)));
    }

    /**
     * Search nodes publish hosted-shards heap only. Nodes that are neither indexing nor search nodes keep the snapshot, because their
     * heap is not part of the routing-table calculation. A node with no snapshot entry is not given one: capacity lives on the snapshot.
     */
    public void testSearchNodeHeapIsUnmodelledAndUnknownNodesAreOmitted() {
        final String indexNode = "index-a";
        final String searchNode = "search-a";
        final String mlNode = "ml-a";
        final String indexName = "test-index";
        final ClusterState clusterState = clusterStateWithRoles(
            indexName,
            Map.of(
                indexNode,
                Set.of(DiscoveryNodeRole.INDEX_ROLE),
                searchNode,
                Set.of(DiscoveryNodeRole.SEARCH_ROLE),
                mlNode,
                Set.of(DiscoveryNodeRole.ML_ROLE)
            ),
            indexNode,
            Map.of(0, searchNode)
        );
        final ShardId shardId = clusterState.routingTable(ProjectId.DEFAULT).index(indexName).shard(0).shardId();
        final long nonShardHeapUsage = 40;
        final NodeHeapEstimates stale = new NodeHeapEstimates(500, 400, nonShardHeapUsage);
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeHeapMetrics(
                Map.of(
                    indexNode,
                    new NodeHeapMetrics(indexNode, 10_000, stale),
                    searchNode,
                    new NodeHeapMetrics(searchNode, 8_000, new NodeHeapEstimates(500, 400, 0)),
                    mlNode,
                    new NodeHeapMetrics(mlNode, 2_000, stale)
                )
            )
            .estimatedShardHeapUsages(Map.of(shardId, new ShardAndIndexHeapUsage(1_200, 200, 1_000)))
            .build();
        final var simulator = new ClusterInfoSimulator(
            TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).build().mutableCloneForSimulation()
        );

        final var nodeHeapUsages = simulator.getClusterInfo().getNodeHeapMetrics();
        // No indexing node hosts a shard, so max postings is 0 and the indexing node is just its non-shard overhead.
        assertThat(
            nodeHeapUsages.get(indexNode).nodeHeapEstimates(),
            equalTo(new NodeHeapEstimates(nonShardHeapUsage, 0, nonShardHeapUsage))
        );
        // hosted = 1200 + 200. Search-node total and non-shard stay unmodelled.
        assertThat(nodeHeapUsages.get(searchNode).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(0, 1_400, 0)));
        assertThat(nodeHeapUsages.get(searchNode).totalBytes(), equalTo(8_000L));
        assertThat(nodeHeapUsages.get(mlNode).nodeHeapEstimates(), equalTo(stale));
        assertThat(nodeHeapUsages.containsKey("not-in-the-snapshot"), equalTo(false));
    }

    /**
     * Indexing nodes share one non-shard overhead while all search nodes publish 0 for this value
     */
    public void testNonShardHeapUsageIsTakenFromIndexingNodes() {
        final String nodeA = "index-a";
        final String nodeB = "index-b";
        final String searchNode = "search-a";
        final long nonShardHeapUsage = 40;
        final ClusterState clusterState = clusterStateWithRoles(
            "test-index",
            Map.of(
                nodeA,
                Set.of(DiscoveryNodeRole.INDEX_ROLE),
                nodeB,
                Set.of(DiscoveryNodeRole.INDEX_ROLE),
                searchNode,
                Set.of(DiscoveryNodeRole.SEARCH_ROLE)
            ),
            nodeA,
            Map.of(0, nodeA)
        );
        final ShardId shardId = clusterState.routingTable(ProjectId.DEFAULT).index("test-index").shard(0).shardId();
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeHeapMetrics(
                Map.of(
                    nodeA,
                    new NodeHeapMetrics(nodeA, 10_000, new NodeHeapEstimates(1, 1, nonShardHeapUsage)),
                    nodeB,
                    new NodeHeapMetrics(nodeB, 10_000, new NodeHeapEstimates(1, 1, nonShardHeapUsage)),
                    searchNode,
                    new NodeHeapMetrics(searchNode, 8_000, new NodeHeapEstimates(0, 0, 0))
                )
            )
            .estimatedShardHeapUsages(Map.of(shardId, new ShardAndIndexHeapUsage(10, 100, 5)))
            .build();
        final var nodeHeapUsages = new ClusterInfoSimulator(
            TestRoutingAllocationFactory.forClusterState(clusterState).clusterInfo(clusterInfo).build().mutableCloneForSimulation()
        ).getClusterInfo().getNodeHeapMetrics();

        // hosted = 10 + 100 = 110. Max postings is 5. Both indexing nodes keep the shared overhead, 40.
        // total on the node that hosts the shard = 40 + (10 - 5) + 100 + 5 = 150
        // total on the empty indexing node = 40 + 5 = 45
        assertThat(nodeHeapUsages.get(nodeA).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(150, 110, nonShardHeapUsage)));
        assertThat(nodeHeapUsages.get(nodeB).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(45, 0, nonShardHeapUsage)));
        assertThat(nodeHeapUsages.get(searchNode).nodeHeapEstimates(), equalTo(new NodeHeapEstimates(0, 0, 0)));
    }

    public void testHeapRecomputeUsesDefaultWhenShardHasNoMetrics() {
        final String nodeA = "index-a";
        final String nodeB = "index-b";
        final ClusterState clusterState = clusterStateWithRoles(
            "test-index",
            Map.of(nodeA, Set.of(DiscoveryNodeRole.INDEX_ROLE), nodeB, Set.of(DiscoveryNodeRole.INDEX_ROLE)),
            nodeA,
            Map.of(0, nodeA)
        );
        final long nonShardHeapUsage = 50;
        final var defaultHeap = new ShardAndIndexHeapUsage(10, 100, 5);
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeHeapMetrics(
                Map.of(
                    nodeA,
                    new NodeHeapMetrics(nodeA, 10_000, new NodeHeapEstimates(9_999, 8_888, nonShardHeapUsage)),
                    nodeB,
                    new NodeHeapMetrics(nodeB, 10_000, new NodeHeapEstimates(9_999, 8_888, nonShardHeapUsage))
                )
            )
            .estimatedShardHeapUsages(Map.of())
            .defaultShardHeapUsageForShardsWithoutMetrics(defaultHeap)
            .build();
        final var allocation = TestRoutingAllocationFactory.forClusterState(clusterState)
            .clusterInfo(clusterInfo)
            .build()
            .mutableCloneForSimulation();
        final var simulator = new ClusterInfoSimulator(allocation);

        // hosted = 10 + 100 = 110; total = 50 + (10 - 5) + 100 + 5 = 160. The empty node is charged the same max postings.
        assertThat(
            simulator.getClusterInfo().getNodeHeapMetrics().get(nodeA).nodeHeapEstimates(),
            equalTo(new NodeHeapEstimates(160, 110, nonShardHeapUsage))
        );
        assertThat(
            simulator.getClusterInfo().getNodeHeapMetrics().get(nodeB).nodeHeapEstimates(),
            equalTo(new NodeHeapEstimates(55, 0, nonShardHeapUsage))
        );

        final ShardId shardId = clusterState.routingTable(ProjectId.DEFAULT).index("test-index").shard(0).shardId();
        final var relocation = allocation.routingNodes()
            .relocateShard(
                allocation.routingNodes().node(nodeA).getByShardId(shardId),
                nodeB,
                0L,
                "relocating shard in test",
                allocation.changes(),
                ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
            );
        allocation.routingNodes().startShard(relocation.v2(), allocation.changes(), 0L);

        assertThat(
            simulator.getClusterInfo().getNodeHeapMetrics().get(nodeA).nodeHeapEstimates(),
            equalTo(new NodeHeapEstimates(55, 0, nonShardHeapUsage))
        );
        assertThat(
            simulator.getClusterInfo().getNodeHeapMetrics().get(nodeB).nodeHeapEstimates(),
            equalTo(new NodeHeapEstimates(160, 110, nonShardHeapUsage))
        );
    }

    public void testEmptyNodeHeapSnapshotStaysEmpty() {
        final ClusterState clusterState = clusterStateWithRoles(
            "test-index",
            Map.of("index-a", Set.of(DiscoveryNodeRole.INDEX_ROLE)),
            "index-a",
            Map.of(0, "index-a")
        );
        final var simulator = new ClusterInfoSimulator(
            TestRoutingAllocationFactory.forClusterState(clusterState).build().mutableCloneForSimulation()
        );
        assertThat(simulator.getClusterInfo().getNodeHeapMetrics().isEmpty(), equalTo(true));
    }

    private static ClusterState clusterStateWithRoles(
        String indexName,
        Map<String, Set<DiscoveryNodeRole>> rolesByNodeId,
        String localNodeId,
        Map<Integer, String> nodeIdByShardId
    ) {
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        rolesByNodeId.forEach((nodeId, roles) -> nodes.add(DiscoveryNodeUtils.builder(nodeId).name(nodeId).roles(roles).build()));
        nodes.localNodeId(localNodeId).masterNodeId(localNodeId);

        final IndexMetadata index = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), nodeIdByShardId.size(), 0))
            .build();
        final IndexRoutingTable.Builder indexRouting = IndexRoutingTable.builder(index.getIndex());
        nodeIdByShardId.forEach(
            (shardId, nodeId) -> indexRouting.addShard(
                shardRoutingBuilder(new ShardId(index.getIndex(), shardId), nodeId, true, STARTED).build()
            )
        );
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(index, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).add(indexRouting))
            .build();
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
        return TestRoutingAllocationFactory.forClusterState(state)
            .allocationDeciders(deciders)
            .clusterInfo(clusterInfo)
            .shardSizeInfo(snapshotShardSizeInfo)
            .mutable();
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
        private final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = new HashMap<>();
        private final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements = new HashMap<>();

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

        public ClusterInfoTestBuilder withNodeCacheSizeAndCommitments(
            String nodeId,
            long cacheSizeInBytes,
            long boostedCacheCommitmentInBytes,
            long unboostedCacheCommitmentInBytes
        ) {
            nodeCacheSizeAndCommitments.put(
                nodeId,
                new NodeCacheSizeAndCommitments(cacheSizeInBytes, boostedCacheCommitmentInBytes, unboostedCacheCommitmentInBytes)
            );
            return this;
        }

        public ClusterInfoTestBuilder withShardCacheRequirement(
            ShardRouting shard,
            long boostedCacheRequirementInBytes,
            long unboostedCacheRequirementInBytes
        ) {
            shardCacheRequirements.put(
                shard.shardId(),
                new BoostedAndUnboostedCacheRequirements(boostedCacheRequirementInBytes, unboostedCacheRequirementInBytes)
            );
            return this;
        }

        public ClusterInfo build() {
            return ClusterInfo.builder()
                .leastAvailableSpaceUsage(leastAvailableSpaceUsage)
                .mostAvailableSpaceUsage(mostAvailableSpaceUsage)
                .shardSizes(shardSizes)
                .reservedSpace(reservedSpace)
                .nodeCacheSizeAndCommitments(nodeCacheSizeAndCommitments)
                .shardCacheRequirements(shardCacheRequirements)
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
