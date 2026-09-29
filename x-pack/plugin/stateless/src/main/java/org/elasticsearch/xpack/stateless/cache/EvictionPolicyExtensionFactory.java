/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.cluster.service.ClusterService;

/**
 * Factory for {@link EvictionPolicyExtension}.
 * The implementation is provided via SPI. When none is present, the NOOP implementation is used.
 */
public interface EvictionPolicyExtensionFactory {

    /**
     * Creates the extension consulted by the shared blob cache on this node.
     * <p>
     * Called once, when the cache service is created. The returned instance is retained for the lifetime of that service
     * and may be invoked concurrently.
     *
     * @param clusterService cluster service of the node creating the cache
     * @return a non-null extension. Return {@link EvictionPolicyExtension#NOOP} to keep the configured window for every shard
     */
    EvictionPolicyExtension create(ClusterService clusterService);

}
