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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.search.OnlinePrewarmingServiceProvider;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class StatelessOnlinePrewarmingServiceProvider implements OnlinePrewarmingServiceProvider {

    private final Stateless plugin;

    public StatelessOnlinePrewarmingServiceProvider() {
        throw new IllegalStateException("This no arg constructor only exists for SPI validation");
    }

    public StatelessOnlinePrewarmingServiceProvider(Stateless plugin) {
        this.plugin = plugin;
    }

    @Override
    public OnlinePrewarmingService create(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        if (DiscoveryNode.hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE)) {
            return new StatelessOnlinePrewarmingService(settings, threadPool, plugin.getStatelessSharedBlobCacheService());
        } else {
            // the index tier can coordinate searches but we must not prewarm the cache
            // on the index tier
            return OnlinePrewarmingService.NOOP;
        }
    }
}
