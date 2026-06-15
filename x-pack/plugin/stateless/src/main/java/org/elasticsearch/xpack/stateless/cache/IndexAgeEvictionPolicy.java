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
 * Eviction policy that prefers keeping cache regions for indices with newer creation timestamps.
 * <p>
 * Richer cache boost and commit-timestamp policies can be implemented as separate classes.
 */
public class IndexAgeEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    @Nullable
    private final ClusterService clusterService;

    public IndexAgeEvictionPolicy(ClusterService clusterService) {
        this.clusterService = Objects.requireNonNull(clusterService);
    }

    // Test subclasses must override {@link #indexCreationDateMillis} because clusterService is null.
    protected IndexAgeEvictionPolicy() {
        this.clusterService = null;
    }

    /**
     * Returns the index creation date in milliseconds since epoch (thus the time-zone is UTC) for the given shard.
     */
    protected long indexCreationDateMillis(ShardId shardId) {
        if (clusterService == null) {
            return Long.MIN_VALUE;
        }
        IndexMetadata metadata = clusterService.state().metadata().findIndex(shardId.getIndex()).orElse(null);
        return metadata != null ? metadata.getCreationDate() : Long.MIN_VALUE;
    }

    @Override
    public boolean canEvict(CacheRegion<FileCacheKey> region, CacheRegion<FileCacheKey> incoming) {
        long regionDate = indexCreationDateMillis(region.key().shardId());
        long incomingDate = indexCreationDateMillis(incoming.key().shardId());
        return regionDate <= incomingDate;
        // When the cache is full of regions from newer indices, older indices might not be able to get a cache region.
        // This is a simplification for now, which can be dealt with in the future.
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {}

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {}
}
