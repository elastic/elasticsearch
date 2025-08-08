/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.getExpectedShardSize;
import static org.elasticsearch.cluster.routing.ExpectedShardSizeEstimator.shouldReserveSpaceForInitializingShard;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class ExpectedShardSizeEstimatorTests extends ESAllocationTestCase {

    private final long defaultValue = randomLongBetween(-1, 0);

    public void testShouldFallbackToDefaultExpectedShardSize() {

        var index = new Index("my-index", randomUUID());
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index(index))).build();
        state = buildRoutingTable(state);
        var shard = shardRoutingBuilder(new ShardId(index, 0), randomIdentifier(), true, ShardRoutingState.INITIALIZING).withRecoverySource(
            randomFrom(RecoverySource.EmptyStoreRecoverySource.INSTANCE, RecoverySource.ExistingStoreRecoverySource.INSTANCE)
        ).build();

        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(defaultValue));
        assertFalse(
            "Should NOT reserve space for locally initializing primaries",
            shouldReserveSpaceForInitializingShard(shard, allocation)
        );
    }

    public void testShouldReadExpectedSizeFromClusterInfo() {

        var shardSize = randomLongBetween(100, 1000);
        var index = new Index("my-index", randomUUID());
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index(index))).build();
        state = buildRoutingTable(state);
        var shard = shardRoutingBuilder(new ShardId(index, 0), randomIdentifier(), true, ShardRoutingState.INITIALIZING).withRecoverySource(
            RecoverySource.PeerRecoverySource.INSTANCE
        ).build();

        var clusterInfo = createClusterInfo(shard, shardSize);
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(shardSize));
        assertTrue("Should reserve space for relocating shard", shouldReserveSpaceForInitializingShard(shard, allocation));
    }

    public void testShouldReadExpectedSizeFromPrimaryWhenAddingNewReplica() {

        var shardSize = randomLongBetween(100, 1000);
        var index = new Index("my-index", randomUUID());
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index(index))).build();
        state = buildRoutingTable(state);
        var shardId = new ShardId(index, 0);
        var primary = newShardRouting(shardId, randomIdentifier(), true, ShardRoutingState.STARTED);
        var replica = newShardRouting(shardId, randomIdentifier(), false, ShardRoutingState.INITIALIZING);

        var clusterInfo = createClusterInfo(primary, shardSize);
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(replica, defaultValue, allocation), equalTo(shardSize));
        assertTrue("Should reserve space for peer recovery", shouldReserveSpaceForInitializingShard(replica, allocation));
    }

    public void testShouldReadExpectedSizeWhenInitializingFromSnapshot() {

        var snapshotShardSize = randomLongBetween(100, 1000);

        var index = new Index("my-index", randomUUID());
        var indexMetadata = switch (randomIntBetween(0, 2)) {
            // regular snapshot
            case 0 -> index(index).settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 0));
            // searchable snapshot
            case 1 -> index(index).settings(
                indexSettings(IndexVersion.current(), index.getUUID(), 1, 0) //
                    .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE) //
            );
            // partial searchable snapshot
            case 2 -> index(index).settings(
                indexSettings(IndexVersion.current(), index.getUUID(), 1, 0) //
                    .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE) //
                    .put(SNAPSHOT_PARTIAL_SETTING.getKey(), true) //
            );
            default -> throw new AssertionError("unexpected index type");
        };
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(indexMetadata)).build();
        state = buildRoutingTable(state);

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        // The uuid in the IndexId is different from index uuid.
        var indexId = new IndexId(index.getName(), randomUUID());

        var shard = shardRoutingBuilder(new ShardId(index, 0), randomIdentifier(), true, ShardRoutingState.INITIALIZING).withRecoverySource(
            new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)
        ).build();

        var snapshotShardSizeInfo = new SnapshotShardSizeInfo(
            Map.of(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shard.shardId()), snapshotShardSize)
        );
        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, snapshotShardSizeInfo);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(snapshotShardSize));
        if (state.metadata().getProject().index(index).isPartialSearchableSnapshot() == false) {
            assertTrue("Should reserve space for snapshot restore", shouldReserveSpaceForInitializingShard(shard, allocation));
        } else {
            assertFalse(
                "Should NOT reserve space for partial searchable snapshot restore as they do not download all data during initialization",
                shouldReserveSpaceForInitializingShard(shard, allocation)
            );
        }
    }

    public void testShouldReadSizeFromClonedShard() {

        Index sourceIndex = new Index("source", randomUUID());
        Index targetIndex = new Index("target", randomUUID());
        var sourceShardSize = randomLongBetween(100, 1000);
        var source = newShardRouting(sourceIndex, 0, randomIdentifier(), true, ShardRoutingState.STARTED);
        var target = shardRoutingBuilder(targetIndex, 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING).withRecoverySource(
            RecoverySource.LocalShardsRecoverySource.INSTANCE
        ).build();

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                metadata(
                    IndexMetadata.builder(sourceIndex.getName())
                        .settings(indexSettings(IndexVersion.current(), sourceIndex.getUUID(), 1, 0)),
                    IndexMetadata.builder(targetIndex.getName())
                        .settings(
                            indexSettings(IndexVersion.current(), targetIndex.getUUID(), 1, 0) //
                                .put(INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex.getName()) //
                                .put(INDEX_RESIZE_SOURCE_UUID_KEY, sourceIndex.getUUID())
                        )
                )
            )
            .routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(source.index()).addShard(source))
                    .add(IndexRoutingTable.builder(target.index()).addShard(target))
            )
            .build();

        var clusterInfo = createClusterInfo(source, sourceShardSize);
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(target, defaultValue, allocation), equalTo(sourceShardSize));
        assertFalse(
            "Should NOT reserve space when using fs hardlink for clone/shrink/split",
            shouldReserveSpaceForInitializingShard(target, state.metadata())
        );
    }

    private static RoutingAllocation createRoutingAllocation(
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo
    ) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), state, clusterInfo, snapshotShardSizeInfo, 0);
    }

    private static IndexMetadata.Builder index(Index index) {
        return IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), index.getUUID(), 1, 0));
    }

    private static Metadata metadata(IndexMetadata.Builder... indices) {
        var builder = Metadata.builder();
        for (IndexMetadata.Builder index : indices) {
            builder.put(index.build(), false);
        }
        return builder.build();
    }

    private static ClusterInfo createClusterInfo(ShardRouting shard, Long size) {
        return ClusterInfo.builder().shardSizes(Map.of(ClusterInfo.shardIdentifierFromRouting(shard), size)).build();
    }

    private ClusterState buildRoutingTable(ClusterState state) {
        ImmutableOpenMap.Builder<ProjectId, RoutingTable> projectRouting = ImmutableOpenMap.builder();
        for (var entry : state.metadata().projects().entrySet()) {
            RoutingTable.Builder builder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            for (var index : entry.getValue()) {
                builder.addAsNew(index);
            }
            projectRouting.put(entry.getKey(), builder.build());
        }
        GlobalRoutingTable routingTable = new GlobalRoutingTable(projectRouting.build());
        return ClusterState.builder(state).routingTable(routingTable).build();
    }

}
