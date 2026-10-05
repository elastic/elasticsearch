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
 * Tracks disk shortfalls for snapshot restores that live allocation could not admit under the
 * restore-disk reserve rule. Written only from {@link org.elasticsearch.xpack.stateless.allocation.SnapshotRestoreAllocationDecider}
 * during live {@code canAllocate}; safe for autoscaling metrics to read.
 * <p>
 * Recorded shortfalls are free-byte deficits (indexing reserve already included).
 * {@link #unmetTotalDiskBytes()} converts that free demand into total disk capacity needed on a new
 * indexing node after the shared blob cache carve-out.
 */
public final class SnapshotRestoreDiskPressure {

    /**
     * Indexing-node shared-cache size applied by {@link StatelessPlugin#additionalSettings()} when unset.
     */
    static final String DEFAULT_INDEXING_SHARED_CACHE_SIZE = "50%";

    private final RelativeByteSizeValue indexingSharedCacheSize;
    private final ConcurrentHashMap<ShardId, Long> unmetDiskShortfalls = new ConcurrentHashMap<>();

    public SnapshotRestoreDiskPressure(Settings settings) {
        this(indexingSharedCacheSize(settings));
    }

    /**
     * @param indexingSharedCacheSize shared-cache size used on indexing nodes (typically 50%);
     *                                used to convert free shortfalls into total disk demand
     */
    public SnapshotRestoreDiskPressure(RelativeByteSizeValue indexingSharedCacheSize) {
        this.indexingSharedCacheSize = indexingSharedCacheSize;
    }

    /**
     * Shared-cache size assumed for new indexing capacity: the explicit setting if present, otherwise
     * the indexing default from {@link StatelessPlugin#additionalSettings()} ({@code 50%}).
     */
    static RelativeByteSizeValue indexingSharedCacheSize(Settings settings) {
        if (SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.exists(settings)) {
            return SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        }
        return RelativeByteSizeValue.parseRelativeByteSizeValue(
            DEFAULT_INDEXING_SHARED_CACHE_SIZE,
            SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
        );
    }

    /**
     * Free bytes currently tracked as unmet for blocked snapshot restores.
     * Reserve is already baked into these values; cache is not.
     */
    public long unmetDiskShortfallBytes() {
        return unmetDiskShortfalls.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Total disk capacity needed so that free space after the indexing shared-cache carve-out
     * covers {@link #unmetDiskShortfallBytes()}.
     * <p>
     * For a relative cache fraction {@code c}: {@code totalDisk = ceil(freeNeeded / (1 - c))}.
     * For an absolute cache size {@code A}: {@code totalDisk = freeNeeded + A}.
     */
    public long unmetTotalDiskBytes() {
        long freeNeeded = unmetDiskShortfallBytes();
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

    /**
     * Snapshot of per-shard unmet free-disk shortfalls.
     */
    public Map<ShardId, Long> unmetDiskShortfalls() {
        return Map.copyOf(unmetDiskShortfalls);
    }

    /**
     * Records a free-byte shortfall for {@code shardId}, keeping the minimum if one was already recorded.
     */
    public void recordShortfall(ShardId shardId, long shortfallBytes) {
        if (shortfallBytes <= 0) {
            throw new IllegalArgumentException("shortfallBytes must be positive, got [" + shortfallBytes + "]");
        }
        unmetDiskShortfalls.merge(shardId, shortfallBytes, Math::min);
    }

    /**
     * Clears any recorded shortfall for {@code shardId}.
     */
    public void clear(ShardId shardId) {
        unmetDiskShortfalls.remove(shardId);
    }

    /**
     * Drops entries whose shard ids are not in {@code liveUnassignedSnapshotPrimaries}.
     */
    public void retainOnly(Set<ShardId> liveUnassignedSnapshotPrimaries) {
        if (unmetDiskShortfalls.isEmpty()) {
            return;
        }
        unmetDiskShortfalls.keySet().removeIf(shardId -> liveUnassignedSnapshotPrimaries.contains(shardId) == false);
    }
}
