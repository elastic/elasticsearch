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
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Objects;

import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING;

/**
 * Factory for stateless shared blob cache eviction policies.
 */
public enum StatelessCacheEvictionPolicyType {
    ALWAYS {
        @Override
        EvictionPolicy<FileCacheKey> doCreate(ClusterService clusterService, IndicesService indicesService, TimeProvider timeProvider) {
            return new DefaultEvictionPolicy<>();
        }
    },
    PINNED_WINDOW {
        @Override
        EvictionPolicy<FileCacheKey> doCreate(ClusterService clusterService, IndicesService indicesService, TimeProvider timeProvider) {
            return new PinnedWindowEvictionPolicy(
                clusterService.getClusterSettings(),
                timeProvider,
                // We consult IndicesService rather than cluster-state routing because routing can lag behind locally open shards
                // during cluster-state application. Once a shard is open here, IndicesService reflects that immediately.
                indicesService.hasShardPredicate()
            );
        }
    },
    INDEX_AGE {
        @Override
        EvictionPolicy<FileCacheKey> doCreate(ClusterService clusterService, IndicesService indicesService, TimeProvider timeProvider) {
            return new IndexAgeEvictionPolicy(clusterService);
        }
    };

    private static final Logger logger = LogManager.getLogger(StatelessCacheEvictionPolicyType.class);

    public final EvictionPolicy<FileCacheKey> create(
        ClusterService clusterService,
        IndicesService indicesService,
        TimeProvider timeProvider
    ) {
        logger.info("creating eviction policy of type [{}]", this);
        return doCreate(clusterService, indicesService, timeProvider);
    }

    abstract EvictionPolicy<FileCacheKey> doCreate(ClusterService clusterService, IndicesService indicesService, TimeProvider timeProvider);

    static StatelessCacheEvictionPolicyType resolveEvictionPolicyFromSettings(Settings settings) {
        // Explicit configuration takes precedence when on search nodes
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)
            && settings.hasValue(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey())) {
            return STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.get(settings);
        }
        // TODO: We ignore eviction policy setting on indexing node for now.
        return defaultEvictionPolicyType(settings);
    }

    static StatelessCacheEvictionPolicyType defaultEvictionPolicyType(Settings settings) {
        // Cache boost preference is disabled: use always evict policy
        if (STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings) == false) {
            return ALWAYS;
        }
        // Default setting value depends on the node role
        return DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE) ? PINNED_WINDOW : ALWAYS;
    }

    public static EvictionPolicy<FileCacheKey> createEvictionPolicy(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        TimeProvider timeProvider
    ) {
        return resolveEvictionPolicyFromSettings(settings).create(
            clusterService,
            Objects.requireNonNull(indicesService),
            Objects.requireNonNull(timeProvider)
        );
    }
}
