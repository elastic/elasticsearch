/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.function.Predicate;

/**
 * An {@link EvictionPolicy} that delegates to a swappable underlying policy, enabling the eviction
 * strategy to be changed at runtime without restarting the node. Only used on search nodes.
 */
class DelegatingEvictionPolicy implements EvictionPolicy<FileCacheKey> {

    private volatile EvictionPolicy<FileCacheKey> delegate;

    DelegatingEvictionPolicy(Settings settings, ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool) {
        assert DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
        clusterService.getClusterSettings()
            .initializeAndWatch(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING,
                newEvictionPolicyType -> this.delegate = newEvictionPolicyType.create(clusterService, indicesService, threadPool)
            );
    }

    EvictionPolicy<FileCacheKey> getDelegate() {
        return delegate;
    }

    @Override
    public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
        return delegate.createPredicate(incoming);
    }

    @Override
    public void onCached(CacheRegion<FileCacheKey> region) {
        delegate.onCached(region);
    }

    @Override
    public void onEvicted(CacheRegion<FileCacheKey> region) {
        delegate.onEvicted(region);
    }
}
