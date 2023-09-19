/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class ShardSizeReaderTests extends ESAllocationTestCase {

    public void testShouldReadExpectedSizeWhenInitializingFromSnapshot() {

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build(), false)
            )
            .build();

        var snapshot = new Snapshot("repository", new SnapshotId("snapshot-1", "na"));
        var indexId = new IndexId("my-index", "_na_");

        var shard = newShardRouting(
            new ShardId("my-index", "_na_", 0),
            randomIdentifier(),
            true,
            ShardRoutingState.INITIALIZING,
            new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
        );

        var shardSize = randomLongBetween(100, 1000);
        var defaultValue = randomLongBetween(-1, 0);

        var snapshotShardSizeInfo = new SnapshotShardSizeInfo(
            Map.of(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shard.shardId()), shardSize)
        );
        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, snapshotShardSizeInfo);

        assertThat(ShardSizeReader.getExpectedShardSize(shard, defaultValue, allocation), equalTo(shardSize));
    }

    public void testShouldReadExpectedSizeFromClusterInfo() {

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build(), false)
            )
            .build();
        var shard = newShardRouting("my-index", 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING);

        var shardSize = randomLongBetween(100, 1000);
        var defaultValue = randomLongBetween(-1, 0);

        var clusterInfo = new ClusterInfo(
            Map.of(),
            Map.of(),
            Map.of(ClusterInfo.shardIdentifierFromRouting(shard), shardSize),
            Map.of(),
            Map.of(),
            Map.of()
        );
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(ShardSizeReader.getExpectedShardSize(shard, defaultValue, allocation), equalTo(shardSize));
    }

    public void testShouldReadExpectedSizeFromForecast() {

        var observedShardSize = randomLongBetween(100, 1000);
        var forecastedShardSize = randomLongBetween(100, 1000);
        var defaultValue = randomLongBetween(-1, 0);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("my-index")
                            .settings(indexSettings(IndexVersion.current(), 1, 0))
                            .shardSizeInBytesForecast(forecastedShardSize)
                            .build(),
                        false
                    )
            )
            .build();
        var shard = newShardRouting("my-index", 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING);

        var clusterInfo = new ClusterInfo(
            Map.of(),
            Map.of(),
            Map.of(ClusterInfo.shardIdentifierFromRouting(shard), observedShardSize),
            Map.of(),
            Map.of(),
            Map.of()
        );
        var allocation = createRoutingAllocation(state, clusterInfo, SnapshotShardSizeInfo.EMPTY);

        assertThat(
            ShardSizeReader.getExpectedShardSize(shard, defaultValue, allocation),
            equalTo(Math.max(observedShardSize, forecastedShardSize))
        );
    }

    public void testShouldFallbackToDefaultValue() {

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder("my-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build(), false)
            )
            .build();
        var shard = newShardRouting("my-index", 0, randomIdentifier(), true, ShardRoutingState.INITIALIZING);
        var defaultValue = randomLongBetween(-1, 0);

        var allocation = createRoutingAllocation(state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY);

        assertThat(ShardSizeReader.getExpectedShardSize(shard, defaultValue, allocation), equalTo(defaultValue));
    }

    private static RoutingAllocation createRoutingAllocation(
        ClusterState state,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo
    ) {
        return new RoutingAllocation(new AllocationDeciders(List.of()), state, clusterInfo, snapshotShardSizeInfo, 0);
    }

}
