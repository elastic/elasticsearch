/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING;

/**
 * Factory for stateless shared blob cache eviction policies.
 */
public enum StatelessCacheEvictionPolicyType {
    ALWAYS {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService) {
            return new DefaultEvictionPolicy<>();
        }
    },
    PINNED_WINDOW {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService) {
            return new PinnedWindowEvictionPolicy(clusterService);
        }
    },
    INDEX_AGE {
        @Override
        EvictionPolicy<FileCacheKey> create(ClusterService clusterService) {
            return new IndexAgeEvictionPolicy(clusterService);
        }
    };

    abstract EvictionPolicy<FileCacheKey> create(ClusterService clusterService);

    static StatelessCacheEvictionPolicyType fromSettings(Settings settings) {
        if (STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings) == false) {
            return ALWAYS;
        }
        return STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.get(settings);
    }
}
