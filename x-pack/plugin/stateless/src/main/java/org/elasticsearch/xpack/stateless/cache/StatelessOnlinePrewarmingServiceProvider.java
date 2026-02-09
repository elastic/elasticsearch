/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
