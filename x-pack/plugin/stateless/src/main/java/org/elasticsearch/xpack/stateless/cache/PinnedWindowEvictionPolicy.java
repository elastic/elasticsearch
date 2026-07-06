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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Eviction policy that does not evict cache regions for shards present on this node whose content timestamp
 * falls within a configurable pinned window.
 * <p>
 * Regions for shards present on this node with {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} are also
 * protected from eviction until a content timestamp is available. This avoids evicting data whose
 * age relative to the pinned window cannot yet be determined.
 */
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
    private final ThreadPool threadPool;

    private volatile TimeValue pinnedWindowDuration = PINNED_WINDOW_DURATION_SETTING.getDefault(Settings.EMPTY);

    public PinnedWindowEvictionPolicy(ClusterSettings clusterSettings, ThreadPool threadPool, Predicate<ShardId> hasShardPredicate) {
        this.hasShardPredicate = Objects.requireNonNull(hasShardPredicate);
        this.threadPool = Objects.requireNonNull(threadPool);
        Objects.requireNonNull(clusterSettings)
            .initializeAndWatchIfRegistered(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value);
    }

    /**
     * For test subclasses that override {@link #hasShard(ShardId)} and optionally {@link #currentTimeMillis()}.
     */
    protected PinnedWindowEvictionPolicy(ThreadPool threadPool, Predicate<ShardId> hasShardPredicate, TimeValue pinnedWindowDuration) {
        this.hasShardPredicate = Objects.requireNonNull(hasShardPredicate);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.pinnedWindowDuration = pinnedWindowDuration;
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

    protected long currentTimeMillis() {
        return threadPool.absoluteTimeInMillis();
    }

    /**
     * Returns {@code true} if {@code timestampMillis} is within the pinned window relative to
     * {@code pinnedWindowCutoffMillis}, inclusive of the window boundary.
     */
    protected boolean isWithinPinnedWindow(long timestampMillis, long pinnedWindowCutoffMillis) {
        return timestampMillis >= pinnedWindowCutoffMillis;
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
        final long pinnedWindowCutoffMillis = currentTimeMillis() - pinnedWindowDuration.getMillis();
        return region -> {
            if (hasShard(region.key().shardId()) == false) {
                return true;
            }
            final long timestampMillis = region.timestampMillis();
            // Protect regions for shards present on this node until their content age can be evaluated.
            // Also protect shards without timestamps.
            if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                return false;
            }
            // TODO: regions of unboosted shards, and of shards with a boost multiplier of less than 1, should be
            // evicted irrespective of their timestamp.
            return isWithinPinnedWindow(timestampMillis, pinnedWindowCutoffMillis) == false;
        };
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
