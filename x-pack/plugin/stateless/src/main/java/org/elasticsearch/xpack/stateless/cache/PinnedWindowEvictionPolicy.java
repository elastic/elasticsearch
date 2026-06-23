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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;
import java.util.function.Predicate;

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

    private final ClusterService clusterService;

    private volatile TimeValue pinnedWindowDuration = PINNED_WINDOW_DURATION_SETTING.getDefault(Settings.EMPTY);

    public PinnedWindowEvictionPolicy(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
        clusterService.getClusterSettings()
            .initializeAndWatchIfRegistered(PINNED_WINDOW_DURATION_SETTING, value -> this.pinnedWindowDuration = value);
    }

    public TimeValue getPinnedWindowDuration() {
        return pinnedWindowDuration;
    }

    /**
     * Returns {@code true} if the shard is assigned to the local {@code routingNode}, including as a relocation target.
     */
    protected boolean isShardLocallyAllocated(ShardId shardId, RoutingNode routingNode) {
        return routingNode.getByShardId(shardId) != null;
    }

    protected long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createEvictionPredicate(CacheRegion<FileCacheKey> incoming) {
        final ClusterState clusterState = clusterService.state();
        final String localNodeId = clusterState.nodes().getLocalNodeId();
        if (localNodeId == null) {
            return Predicates.always();
        }
        final RoutingNode localRoutingNode = clusterState.getRoutingNodes().node(localNodeId);
        if (localRoutingNode == null) {
            return Predicates.always();
        }
        final long pinnedWindowCutoffMillis = currentTimeMillis() - pinnedWindowDuration.getMillis();
        return region -> {
            if (isShardLocallyAllocated(region.key().shardId(), localRoutingNode) == false) {
                return true;
            }
            final long timestampMillis = region.timestampMillis();
            // Protect locally allocated regions until their content age can be evaluated.
            if (timestampMillis == SharedBlobCacheService.UNKNOWN_TIMESTAMP) {
                return false;
            }
            return timestampMillis < pinnedWindowCutoffMillis;
        };
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
