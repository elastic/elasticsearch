/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.OnlinePrewarmingServiceProvider;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;

public class StatelessOnlinePrewarmingServiceProvider implements OnlinePrewarmingServiceProvider {

    private final StatelessPlugin plugin;

    public StatelessOnlinePrewarmingServiceProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessOnlinePrewarmingServiceProvider(StatelessPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public OnlinePrewarmingService create(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TelemetryProvider provider
    ) {
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)) {
            return new StatelessOnlinePrewarmingService(
                settings,
                threadPool,
                plugin.getStatelessSharedBlobCacheService(),
                provider.getMeterRegistry()
            );
        } else {
            // the index tier can coordinate searches but we must not prewarm the cache
            // on the index tier
            return OnlinePrewarmingService.NOOP;
        }
    }
}
