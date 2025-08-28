/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;

import java.util.Map;

/**
 * Collects the thread pool usage stats for each node in the cluster.
 * <p>
 * Results are returned as a map of node ID to node usage stats.
 */
public class NodeUsageStatsForThreadPoolsCollector {
    public static final NodeUsageStatsForThreadPoolsCollector EMPTY = new NodeUsageStatsForThreadPoolsCollector() {
        public void collectUsageStats(
            Client client,
            ClusterState clusterState,
            ActionListener<Map<String, NodeUsageStatsForThreadPools>> listener
        ) {
            listener.onResponse(Map.of());
        }
    };

    private static final TransportVersion TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION = TransportVersion.fromName(
        "transport_node_usage_stats_for_thread_pools_action"
    );

    /**
     * Collects the thread pool usage stats ({@link NodeUsageStatsForThreadPools}) for each node in the cluster.
     *
     * @param listener The listener to receive the usage results.
     */
    public void collectUsageStats(
        Client client,
        ClusterState clusterState,
        ActionListener<Map<String, NodeUsageStatsForThreadPools>> listener
    ) {
        var dataNodeIds = clusterState.nodes().getDataNodes().values().stream().map(node -> node.getId()).toArray(String[]::new);
        if (clusterState.getMinTransportVersion().supports(TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION)) {
            client.execute(
                TransportNodeUsageStatsForThreadPoolsAction.TYPE,
                new NodeUsageStatsForThreadPoolsAction.Request(dataNodeIds),
                listener.map(response -> response.getAllNodeUsageStatsForThreadPools())
            );
        } else {
            listener.onResponse(Map.of());
        }
    }
}
