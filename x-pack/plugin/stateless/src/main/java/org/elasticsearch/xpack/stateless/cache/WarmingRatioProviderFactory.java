/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.index.shard.ShardId;

/**
 * Factory for creating {@link WarmingRatioProvider} instances.
 * The exact implementation is provided via SPI. If none is provided, {@link DefaultWarmingRatioProviderFactory} is used.
 */
public interface WarmingRatioProviderFactory {
    /**
     * Invoked once per search shard recovery; implementations must not register settings consumers.
     * Read the current setting values instead.
     */
    WarmingRatioProvider create(ShardId shardId);
}
