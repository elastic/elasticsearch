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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.utils.ClusterUtils;

import java.util.Objects;

/**
 * Eviction policy that does not evict locally allocated cache regions whose content timestamp
 * falls within a configurable pinned window.
 * <p>
 * Regions for locally allocated shards with {@link SharedBlobCacheService#UNKNOWN_TIMESTAMP} are
 * also protected from eviction until a content timestamp is available. This avoids evicting data
 * whose age relative to the pinned window cannot yet be determined.
 */
public class PinnedWindowEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    /**
     * Configures the pinned-window duration for non-evictable data when cache boost preference is enabled.
     */
    public static final Setting<TimeValue> PINNED_WINDOW_DURATION_SETTING = Setting.timeSetting(
        "stateless.cache_boost_preference.pinned_window.duration",
        TimeValue.timeValueDays(1),
        TimeValue.timeValueSeconds(1),
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    @Nullable
    private final ClusterService clusterService;

    private volatile TimeValue pinnedWindowDuration = PINNED_WINDOW_DURATION_SETTING.getDefault(Settings.EMPTY);

    public PinnedWindowEvictionPolicy(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value);
    }

    /**
     * For test subclasses that override {@link #isShardLocallyAllocated} and optionally {@link #currentTimeMillis()}.
     */
    protected PinnedWindowEvictionPolicy(TimeValue pinnedWindowDuration) {
        this.clusterService = null;
        this.pinnedWindowDuration = pinnedWindowDuration;
    }

    public TimeValue getPinnedWindowDuration() {
        return pinnedWindowDuration;
    }

    /**
     * Returns {@code true} if the shard is assigned to the local node, including as a relocation target.
     */
    protected boolean isShardLocallyAllocated(ShardId shardId) {
        assert clusterService != null;
        return ClusterUtils.isShardLocallyAllocated(clusterService, shardId);
    }

    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Returns {@code true} if {@code timestampMillis} is within the pinned window relative to {@link #currentTimeMillis()},
     * inclusive of the window boundary.
     */
    protected boolean isWithinPinnedWindow(long timestampMillis) {
        return currentTimeMillis() - timestampMillis <= pinnedWindowDuration.getMillis();
    }

    @Override
    public boolean canEvict(CacheRegion<FileCacheKey> region, CacheRegion<FileCacheKey> incoming) {
        if (isShardLocallyAllocated(region.key().shardId()) == false) {
            return true;
        }
        final long timestampMillis = region.timestampMillis();
        // Protect locally allocated regions until their content age can be evaluated.
        if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
            return false;
        }
        return isWithinPinnedWindow(timestampMillis) == false;
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
