/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;

import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING;

/**
 * Factory for stateless shared blob cache eviction policies.
 */
public enum StatelessCacheEvictionPolicyType {
    ALWAYS {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool) {
            return new DefaultEvictionPolicy<>();
        }
    },
    PINNED_WINDOW {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool) {
            return new PinnedWindowEvictionPolicy(
                clusterService.getClusterSettings(),
                threadPool,
                // We consult IndicesService rather than cluster-state routing because routing can lag behind locally open shards
                // during cluster-state application. Once a shard is open here, IndicesService reflects that immediately.
                indicesService.hasShardPredicate()
            );
        }
    },
    INDEX_AGE {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool) {
            return new IndexAgeEvictionPolicy(clusterService);
        }
    };

    abstract EvictionPolicy<FileCacheKey> create(ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool);

    static StatelessCacheEvictionPolicyType resolveEvictionPolicyFromSettings(Settings settings) {
        if (STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings) == false) {
            return ALWAYS;
        }
        if (settings.hasValue(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.getKey())) {
            return STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.get(settings);
        }
        return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE) ? PINNED_WINDOW : ALWAYS;
    }

    static EvictionPolicy<FileCacheKey> createEvictionPolicy(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool
    ) {
        return resolveEvictionPolicyFromSettings(settings).create(
            clusterService,
            Objects.requireNonNull(indicesService),
            Objects.requireNonNull(threadPool)
        );
    }
}
