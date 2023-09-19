/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Set;

public class ShardSizeReader {

    public static long getExpectedShardSize(ShardRouting shard, long defaultValue, RoutingAllocation allocation) {
        return getExpectedShardSize(
            shard,
            defaultValue,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    /**
     * Returns the expected shard size for the given shard. It is estimated using following:
     * * source shards size when initializing using split/shrink/clone operation
     * * source shard size when initializing from snapshot
     * * forecasted shard size
     * * shard size from ClusterInfo if available
     */
    public static long getExpectedShardSize(
        ShardRouting shard,
        long defaultValue,
        ClusterInfo clusterInfo,
        SnapshotShardSizeInfo snapshotShardSizeInfo,
        Metadata metadata,
        RoutingTable routingTable
    ) {
        final IndexMetadata indexMetadata = metadata.getIndexSafe(shard.index());
        if (shard.active() == false
            && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS
            && indexMetadata.getResizeSourceIndex() != null) {
            return extracted(shard, defaultValue, indexMetadata, clusterInfo, metadata, routingTable);
        } else if (shard.active() == false && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT && shard.primary()) {
            return snapshotShardSizeInfo.getShardSize(shard, defaultValue);
        } else if (indexMetadata.getForecastedShardSizeInBytes().isPresent()) {
            return Math.max(indexMetadata.getForecastedShardSizeInBytes().getAsLong(), clusterInfo.getShardSize(shard, defaultValue));
        } else {
            return clusterInfo.getShardSize(shard, defaultValue);
        }
    }

    private static long extracted(
        ShardRouting shard,
        long defaultValue,
        IndexMetadata indexMetadata,
        ClusterInfo clusterInfo,
        Metadata metadata,
        RoutingTable routingTable
    ) {
        // in the shrink index case we sum up the source index shards since we basically make a copy of the shard in the worst case
        long targetShardSize = 0;
        final Index mergeSourceIndex = indexMetadata.getResizeSourceIndex();
        final IndexMetadata sourceIndexMetadata = metadata.index(mergeSourceIndex);
        if (sourceIndexMetadata != null) {
            final Set<ShardId> shardIds = IndexMetadata.selectRecoverFromShards(
                shard.id(),
                sourceIndexMetadata,
                indexMetadata.getNumberOfShards()
            );
            final IndexRoutingTable indexRoutingTable = routingTable.index(mergeSourceIndex.getName());
            for (int i = 0; i < indexRoutingTable.size(); i++) {
                IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
                if (shardIds.contains(shardRoutingTable.shardId())) {
                    targetShardSize += clusterInfo.getShardSize(shardRoutingTable.primaryShard(), 0);
                }
            }
        }
        return targetShardSize == 0 ? defaultValue : targetShardSize;
    }
}
