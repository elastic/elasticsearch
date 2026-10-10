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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks free-disk shortfalls for snapshot restores that live allocation could not admit.
 * Written from {@link org.elasticsearch.xpack.stateless.allocation.SnapshotRestoreAllocationDecider};
 * autoscaling reads {@link #unmetTotalDiskBytes()}.
 */
public final class SnapshotRestoreDiskPressure {

    private static final String DEFAULT_INDEXING_SHARED_CACHE_SIZE = "50%";

    private final RelativeByteSizeValue indexingSharedCacheSize;
    private final ConcurrentHashMap<ShardId, Long> unmetDiskShortfalls = new ConcurrentHashMap<>();

    public SnapshotRestoreDiskPressure(Settings settings) {
        this.indexingSharedCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings)
            ? SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
            : RelativeByteSizeValue.parseRelativeByteSizeValue(
                DEFAULT_INDEXING_SHARED_CACHE_SIZE,
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
            );
    }

    /**
     * Total disk needed on a new indexing node so free space after the shared-cache carve-out
     * covers recorded free shortfalls (indexing reserve already included).
     */
    public long unmetTotalDiskBytes() {
        long freeNeeded = unmetDiskShortfalls.values().stream().mapToLong(Long::longValue).sum();
        if (freeNeeded == 0L) {
            return 0L;
        }
        if (indexingSharedCacheSize.isAbsolute()) {
            return freeNeeded + indexingSharedCacheSize.getAbsolute().getBytes();
        }
        double freeRatio = 1.0d - indexingSharedCacheSize.getRatio().getAsRatio();
        if (freeRatio <= 0.0d) {
            throw new IllegalStateException(
                "indexing shared cache leaves no free disk for restore capacity [cache=" + indexingSharedCacheSize.getStringRep() + "]"
            );
        }
        return (long) Math.ceil(freeNeeded / freeRatio);
    }

    public void recordShortfall(ShardId shardId, long shortfallBytes) {
        if (shortfallBytes <= 0) {
            throw new IllegalArgumentException("shortfallBytes must be positive, got [" + shortfallBytes + "]");
        }
        unmetDiskShortfalls.merge(shardId, shortfallBytes, Math::min);
    }

    public void clear(ShardId shardId) {
        unmetDiskShortfalls.remove(shardId);
    }

    public void retainOnly(Set<ShardId> liveUnassignedSnapshotPrimaries) {
        if (unmetDiskShortfalls.isEmpty()) {
            return;
        }
        unmetDiskShortfalls.keySet().removeIf(shardId -> liveUnassignedSnapshotPrimaries.contains(shardId) == false);
    }
}
