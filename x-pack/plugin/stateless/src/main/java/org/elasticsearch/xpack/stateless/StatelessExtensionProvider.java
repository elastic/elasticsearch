/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.utils.SearchShardSizeCollector;

/**
 * SPI for extending plugins that need to receive stateless services from the stateless plugin.
 */
public interface StatelessExtensionProvider {

    /**
     * Callback invoked for extending plugins to allow them to access the stateless services.
     */
    default void onServicesCreated(
        ClosedShardService closedShardService,
        HollowShardsService hollowShardsService,
        SearchShardSizeCollector searchShardSizeCollector,
        StatelessMemoryMetricsService statelessMemoryMetricsService,
        ObjectStoreService objectStoreService
    ) {}
}
