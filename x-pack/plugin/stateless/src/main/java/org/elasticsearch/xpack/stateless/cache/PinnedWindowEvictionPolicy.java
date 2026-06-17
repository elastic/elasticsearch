/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;

/**
 * Eviction policy that does not evict data within a configurable pinned window.
 * <p>
 * TODO: Complete pinned-window eviction behavior in a follow-up PR. Until then, all regions are evictable.
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

    private volatile TimeValue pinnedWindowDuration;

    public PinnedWindowEvictionPolicy(ClusterService clusterService) {
        Objects.requireNonNull(clusterService);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value);
    }

    public TimeValue getPinnedWindowDuration() {
        return pinnedWindowDuration;
    }

    @Override
    public boolean canEvict(CacheRegion<FileCacheKey> region, CacheRegion<FileCacheKey> incoming) {
        // TODO: Respect pinned window duration for valid shards.
        return true;
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
