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
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

/**
 * Default {@link EvictionPolicyFactory} when no SPI implementation is registered.
 * Every region is eligible for eviction.
 */
public class DefaultEvictionPolicyFactory implements EvictionPolicyFactory {

    @Override
    public EvictionPolicy<FileCacheKey> create(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        TimeProvider timeProvider
    ) {
        return new DefaultEvictionPolicy<>();
    }
}
