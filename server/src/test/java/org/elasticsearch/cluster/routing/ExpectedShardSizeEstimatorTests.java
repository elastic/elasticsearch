/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
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
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ExpectedShardSizeEstimatorTests extends ESAllocationTestCase {

    private final long defaultValue = randomLongBetween(-1, 0);

    public void testShouldFallbackToDefaultValue() {

        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index("my-index"))).build();
        var shard = newShardRouting("my-index", 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING);

        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(defaultValue));
    }

    public void testShouldReadExpectedSizeFromClusterInfo() {

        var shardSize = randomLongBetween(100, 1000);
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index("my-index"))).build();
        var shard = newShardRouting("my-index", 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING);

        var clusterInfo = createClusterInfo(shard, shardSize);
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(shardSize));
    }

    public void testShouldReadExpectedSizeWhenInitializingFromSnapshot() {

        var snapshotShardSize = randomLongBetween(100, 1000);
        var state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata(index("my-index"))).build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");

        var shard = newShardRouting(
            new ShardId("my-index", "_na_", 0),
            null,
            true,
            ShardRoutingState.UNASSIGNED,
            new RecoverySource.SnapshotRecoverySource(randomUUID(), snapshot, IndexVersion.current(), indexId)
        );

        var snapshotShardSizeInfo = new SnapshotShardSizeInfo(
            Map.of(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shard.shardId()), snapshotShardSize)
        );
        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, snapshotShardSizeInfo);

        assertThat(getExpectedShardSize(shard, defaultValue, allocation), equalTo(snapshotShardSize));
    }

    public void testShouldReadSizeFromClonedShard() {

        var sourceShardSize = randomLongBetween(100, 1000);
        var source = newShardRouting(new ShardId("source", "_na_", 0), randomIdentifier(), true, ShardRoutingState.STARTED);
        var target = newShardRouting(
            new ShardId("target", "_na_", 0),
            randomIdentifier(),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.LocalShardsRecoverySource.INSTANCE
        );

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                metadata(
                    IndexMetadata.builder("source").settings(indexSettings(IndexVersion.current(), 2, 0)),
                    IndexMetadata.builder("target")
                        .settings(
                            indexSettings(IndexVersion.current(), 1, 0) //
                                .put(INDEX_RESIZE_SOURCE_NAME_KEY, "source") //
                                .put(INDEX_RESIZE_SOURCE_UUID_KEY, "_na_")
                        )
                )
            )
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(source.index()).addShard(source)))
            .build();

        var clusterInfo = createClusterInfo(source, sourceShardSize);
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(getExpectedShardSize(target, defaultValue, allocation), equalTo(sourceShardSize));
    }

    private static RoutingAllocation createRoutingAllocation(
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo
    ) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), state, clusterInfo, snapshotShardSizeInfo, 0);
    }

    private static IndexMetadata.Builder index(String name) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0));
    }

    private static Metadata metadata(IndexMetadata.Builder... indices) {
        var builder = Metadata.builder();
        for (IndexMetadata.Builder index : indices) {
            builder.put(index.build(), false);
        }
        return builder.build();
    }

    private static ClusterInfo createClusterInfo(ShardRouting shard, Long size) {
        return new ClusterInfo(
            Map.of(),
            Map.of(),
            Map.of(ClusterInfo.shardIdentifierFromRouting(shard), size),
            Map.of(),
            Map.of(),
            Map.of()
        );
    }
}
