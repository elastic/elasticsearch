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

    public static boolean shouldReserveSpaceForInitializingShard(ShardRouting shard, RoutingAllocation allocation) {
        return shouldReserveSpaceForInitializingShard(shard, allocation.metadata());
    }

    public static long getExpectedShardSize(ShardRouting shard, long defaultSize, RoutingAllocation allocation) {
        return getExpectedShardSize(
            shard,
            defaultSize,
            allocation.clusterInfo(),
            allocation.snapshotShardSizeInfo(),
            allocation.metadata(),
            allocation.routingTable()
        );
    }

    public static boolean shouldReserveSpaceForInitializingShard(ShardRouting shard, Metadata metadata) {
        assert shard.initializing() : "Expected initializing shard, got: " + shard;
        return switch (shard.recoverySource().getType()) {
            // No need to reserve disk space when initializing a new empty shard
            case EMPTY_STORE -> false;

            // No need to reserve disk space if the shard is already allocated on the disk. Starting it is not going to use more.
            case EXISTING_STORE -> false;

            // Peer recovery require downloading all segments locally to start the shard. Reserve disk space for this
            case PEER -> true;

            // Snapshot restore (unless it is partial) require downloading all segments locally from the blobstore to start the shard.
            // See org.elasticsearch.xpack.searchablesnapshots.action.TransportMountSearchableSnapshotAction.buildIndexSettings
            // and DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS
            case SNAPSHOT -> metadata.getIndexSafe(shard.index()).isPartialSearchableSnapshot() == false;

            // shrink/split/clone operation is going to clone existing locally placed shards using file system hard links
            // so no additional space is going to be used until future merges
            case LOCAL_SHARDS -> false;
        };
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
        assert defaultValue == 0L || defaultValue == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
            : "Unexpected default value for expected shard size: " + defaultValue;
        final IndexMetadata indexMetadata = metadata.getIndexSafe(shard.index());
        if (indexMetadata.getResizeSourceIndex() != null
            && shard.active() == false
            && shard.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            assert shard.primary() : "All replica shards are recovering from " + RecoverySource.Type.PEER;
            return getExpectedSizeOfResizedShard(shard, defaultValue, indexMetadata, clusterInfo, metadata, routingTable);
        } else if (shard.active() == false && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
            assert shard.primary() : "All replica shards are recovering from " + RecoverySource.Type.PEER;
            return snapshotShardSizeInfo.getShardSize(shard, defaultValue);
        } else {
            var shardSize = clusterInfo.getShardSize(shard.shardId(), shard.primary());
            if (shardSize == null && shard.primary() == false) {
                // derive replica size from corresponding primary
                shardSize = clusterInfo.getShardSize(shard.shardId(), true);
            }
            return shardSize == null ? defaultValue : shardSize;
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
