/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;

/**
 * Eviction policy that evicts cache regions according to cache boost preferences.
 *
 * NB: For the moment, the policy simply prefers keeping cache regions for indices with newer creation timestamps. This will
 * change when the cache boost preferences and timestamp information are defined/accessible.
 */
public class StatelessEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    @Nullable
    private final ClusterService clusterService;

    public StatelessEvictionPolicy(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    // for tests that override {@link #indexCreationDateMillis}
    protected StatelessEvictionPolicy() {
        this.clusterService = null;
    }

    /**
     * Returns the index creation date in milliseconds for the given shard.
     */
    protected long indexCreationDateMillis(ShardId shardId) {
        final ClusterService clusterService = Objects.requireNonNull(this.clusterService, "clusterService must be set");
        IndexMetadata metadata = clusterService.state().metadata().findIndex(shardId.getIndex()).orElse(null);
        return metadata != null ? metadata.getCreationDate() : Long.MIN_VALUE;
    }

    @Override
    public boolean canEvict(CacheRegion<FileCacheKey> region, CacheRegion<FileCacheKey> incoming) {
        long regionDate = indexCreationDateMillis(region.key().shardId());
        long incomingDate = indexCreationDateMillis(incoming.key().shardId());
        return regionDate <= incomingDate;
        // When the cache is full of regions from newer indices, older indices might not be able to get a cache region.
        // This is a simplification for now, which will be dealt with in the future.
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
