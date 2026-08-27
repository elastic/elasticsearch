/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;

/**
 * Factory for creating {@link WarmingRatioProvider} instances.
 * The exact implementation is provided via SPI. If none is provided, {@link DefaultWarmingRatioProviderFactory} is used.
 */
public interface WarmingRatioProviderFactory {
    WarmingRatioProvider create(ClusterSettings clusterSettings);
}
