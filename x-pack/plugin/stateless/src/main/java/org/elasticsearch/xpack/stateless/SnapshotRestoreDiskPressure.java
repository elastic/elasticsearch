/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks snapshot restore shards that live allocation could not admit because of disk capacity.
 * Written from {@link org.elasticsearch.xpack.stateless.allocation.SnapshotRestoreAllocationDecider};
 * autoscaling reads {@link #restoreShardSizes()}.
 */
public final class SnapshotRestoreDiskPressure {

    private static final String DEFAULT_INDEXING_SHARED_CACHE_SIZE = "50%";

    private final RelativeByteSizeValue indexingSharedCacheSize;
    private final RelativeByteSizeValue indexingReservedDisk;
    private final ConcurrentHashMap<ShardId, Long> unplaceableShardSizes = new ConcurrentHashMap<>();

    public SnapshotRestoreDiskPressure(Settings settings) {
        this.indexingSharedCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings)
            ? SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
            : RelativeByteSizeValue.parseRelativeByteSizeValue(
                DEFAULT_INDEXING_SHARED_CACHE_SIZE,
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
            );
        this.indexingReservedDisk = IndexingDiskController.INDEXING_DISK_RESERVED_BYTES_SETTING.get(settings);
    }

    /**
     * Returns one immutable snapshot of the unplaceable restore shards and the disk settings used to evaluate them.
     */
    public RestoreShardSizes restoreShardSizes() {
        return new RestoreShardSizes(unplaceableShardSizes, indexingSharedCacheSize, indexingReservedDisk);
    }

    public void recordShardSize(ShardId shardId, long shardSizeBytes) {
        if (shardSizeBytes < 0) {
            throw new IllegalArgumentException("shardSizeBytes must be non-negative, got [" + shardSizeBytes + "]");
        }
        unplaceableShardSizes.put(shardId, shardSizeBytes);
    }

    public void clear(ShardId shardId) {
        unplaceableShardSizes.remove(shardId);
    }

    public void retainOnly(Set<ShardId> liveUnassignedSnapshotPrimaries) {
        if (unplaceableShardSizes.isEmpty()) {
            return;
        }
        unplaceableShardSizes.keySet().removeIf(shardId -> liveUnassignedSnapshotPrimaries.contains(shardId) == false);
    }

    /**
     * Inputs used by serverless autoscaling to calculate aggregate restore demand and a minimum feasible node size.
     */
    public record RestoreShardSizes(
        Map<ShardId, Long> shardSizes,
        RelativeByteSizeValue indexingSharedCacheSize,
        RelativeByteSizeValue indexingReservedDisk
    ) {
        public RestoreShardSizes {
            shardSizes = Map.copyOf(shardSizes);
        }
    }
}
