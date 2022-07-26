/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Map;

public class SnapshotShardSizeInfo {

    public static final SnapshotShardSizeInfo EMPTY = new SnapshotShardSizeInfo(Map.of());

    private final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes;

    public SnapshotShardSizeInfo(Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes) {
        this.snapshotShardSizes = snapshotShardSizes;
    }

    public Long getShardSize(ShardRouting shardRouting) {
        if (shardRouting.primary()
            && shardRouting.active() == false
            && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
            final RecoverySource.SnapshotRecoverySource snapshotRecoverySource = (RecoverySource.SnapshotRecoverySource) shardRouting
                .recoverySource();
            return snapshotShardSizes.get(
                new InternalSnapshotsInfoService.SnapshotShard(
                    snapshotRecoverySource.snapshot(),
                    snapshotRecoverySource.index(),
                    shardRouting.shardId()
                )
            );
        }
        assert false : "Expected shard with snapshot recovery source but was " + shardRouting;
        return null;
    }

    public long getShardSize(ShardRouting shardRouting, long fallback) {
        final Long shardSize = getShardSize(shardRouting);
        if (shardSize == null || shardSize == ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            return fallback;
        }
        return shardSize;
    }
}
