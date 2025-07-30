/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Collection;

import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeStatsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testNodeStatsIncludesThreadPoolUtilization() {
        final String dataNode = internalCluster().startNode();

        indexRandom(true, randomIdentifier(), randomIntBetween(10, 100));

        // Trigger a collection (we piggyback on the APM utilization measure for now)
        for (PluginsService pluginsService : internalCluster().getInstances(PluginsService.class)) {
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();
            telemetryPlugin.collect();
        }

        final NodesStatsResponse nodesStatsResponse = internalCluster().client()
            .admin()
            .cluster()
            .prepareNodesStats(dataNode)
            .setThreadPool(true)
            .execute()
            .actionGet();

        final ThreadPoolStats threadPool = nodesStatsResponse.getNodes().getFirst().getThreadPool();
        final ThreadPoolStats.Stats writeThreadPoolStats = threadPool.stats()
            .stream()
            .filter(st -> ThreadPool.Names.WRITE.equals(st.name()))
            .findFirst()
            .orElseThrow();
        assertThat(writeThreadPoolStats.utilization(), greaterThan(0.0f));
    }
}
