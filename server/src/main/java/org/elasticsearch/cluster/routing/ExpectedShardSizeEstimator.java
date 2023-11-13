/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Set;

public class ExpectedShardSizeEstimator {

    public static long getExpectedShardSize(ShardRouting shardRouting, long defaultSize, RoutingAllocation allocation) {
        return getExpectedShardSize(
            shardRouting,
            defaultSize,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    /**
     * Returns the expected shard size for the given shard or the default value provided if not enough information are available
     * to estimate the shards size.
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
        if (indexMetadata.getResizeSourceIndex() != null
            && shard.active() == false
            && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            return getExpectedSizeOfResizedShard(shard, defaultValue, indexMetadata, clusterInfo, metadata, routingTable);
        } else if (shard.unassigned() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
            return snapshotShardSizeInfo.getShardSize(shard, defaultValue);
        } else {
            return clusterInfo.getShardSize(shard, defaultValue);
        }
    }

    private static long getExpectedSizeOfResizedShard(
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
