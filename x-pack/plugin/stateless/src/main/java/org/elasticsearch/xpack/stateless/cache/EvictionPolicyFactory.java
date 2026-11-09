/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

/**
 * Factory for the shared blob cache {@link EvictionPolicy}.
 * The exact implementation is provided via SPI. If none is provided, {@link DefaultEvictionPolicyFactory} is used.
 */
public interface EvictionPolicyFactory {

    /**
     * Creates the eviction policy for this node.
     * <p>
     * The returned policy may watch cluster settings and must be closed with the cache.
     */
    EvictionPolicy<FileCacheKey> create(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        TimeProvider timeProvider
    );
}
