/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.Predicate;

/// Eviction policy that does not evict cache regions for shards present on this node whose content timestamp
/// falls within a configurable pinned window.
///
/// Regions are classified by their [CacheRegion#timestampMillis()] (for shards present on this node):
///   - a non-negative timestamp (`>= 0`) is pinned iff it falls within the pinned window;
///   - [SharedBlobCacheService#UNKNOWN_TIMESTAMP] is always pinned (no representative timestamp);
///   - [SharedBlobCacheService#BACKFILL_IN_PROGRESS_TIMESTAMP] is always pinned until backfill completes.
///
public class PinnedWindowEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    /**
     * Configures the pinned-window duration for non-evictable data when cache boost preference is enabled.
     */
    public static final Setting<TimeValue> PINNED_WINDOW_DURATION_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.pinned_window.duration",
        TimeValue.timeValueHours(12),
        TimeValue.timeValueSeconds(1),
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private final Predicate<ShardId> hasShardPredicate;
    private final TimeProvider timeProvider;

    private volatile TimeValue pinnedWindowDuration;

    private final Releasable releasePinnedWindowDurationUpdater;

    public PinnedWindowEvictionPolicy(ClusterSettings clusterSettings, TimeProvider timeProvider, Predicate<ShardId> hasShardPredicate) {
        this.hasShardPredicate = Objects.requireNonNull(hasShardPredicate);
        this.timeProvider = Objects.requireNonNull(timeProvider);
        this.pinnedWindowDuration = Objects.requireNonNull(clusterSettings).get(PINNED_WINDOW_DURATION_SETTING);
        this.releasePinnedWindowDurationUpdater = Releasables.releaseOnce(
            clusterSettings.addRemovableSettingsUpdateConsumer(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value)
        );
    }

    public TimeValue getPinnedWindowDuration() {
        return pinnedWindowDuration;
    }

    /**
     * Returns {@code true} if the shard is present on this node.
     */
    protected boolean hasShard(ShardId shardId) {
        return hasShardPredicate.test(shardId);
    }

    private long currentTimeMillis() {
        return timeProvider.absoluteTimeInMillis();
    }

    /**
     * Returns {@code true} if {@code region} is currently protected from eviction by this policy based on its own
     * state (independent of any incoming region).
     */
    @Override
    public boolean isProtected(CacheRegion<FileCacheKey> region) {
        return isProtected(region, currentTimeMillis() - pinnedWindowDuration.getMillis());
    }

    /**
     * Returns {@code true} if {@code region} is currently protected from eviction by this policy based on its own
     * state (independent of any incoming region), using a precomputed pinned-window cutoff.
     */
    boolean isProtected(CacheRegion<FileCacheKey> region, long pinnedWindowCutoffMillis) {
        if (hasShard(region.key().shardId()) == false) {
            return false;
        }
        final long timestampMillis = region.timestampMillis();
        if (timestampMillis < 0) {
            assert timestampMillis == SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP
                || timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP : "unexpected negative timestamp: " + timestampMillis;
            return true;
        }
        // TODO: regions of unboosted shards, and of shards with a boost multiplier of less than 1, should be
        // evicted irrespective of their timestamp.
        return timestampMillis >= pinnedWindowCutoffMillis;
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
        final long pinnedWindowCutoffMillis = currentTimeMillis() - pinnedWindowDuration.getMillis();
        return region -> isProtected(region, pinnedWindowCutoffMillis) == false;
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}

    @Override
    public void close() {
        // Remove the pinned window duration updater from ClusterSettings registration
        Releasables.close(releasePinnedWindowDurationUpdater);
    }
}
