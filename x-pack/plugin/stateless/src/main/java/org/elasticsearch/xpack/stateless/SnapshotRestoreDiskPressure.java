/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks disk shortfalls for snapshot restores that live allocation could not admit under the
 * restore-disk reserve rule. Written only from {@link org.elasticsearch.xpack.stateless.allocation.SnapshotRestoreAllocationDecider}
 * during live {@code canAllocate}; safe for autoscaling metrics to read.
 */
public final class SnapshotRestoreDiskPressure {

    private final ConcurrentHashMap<ShardId, Long> unmetDiskShortfalls = new ConcurrentHashMap<>();

    /**
     * Disk bytes currently tracked as unmet for blocked snapshot restores.
     */
    public long unmetDiskShortfallBytes() {
        return unmetDiskShortfalls.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Snapshot of per-shard unmet disk shortfalls.
     */
    public Map<ShardId, Long> unmetDiskShortfalls() {
        return Map.copyOf(unmetDiskShortfalls);
    }

    /**
     * Records a shortfall for {@code shardId}, keeping the minimum if one was already recorded.
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
