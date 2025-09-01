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
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collects the thread pool usage stats for each node in the cluster.
 * <p>
 * Results are returned as a map of node ID to node usage stats. Keeps track of the most recent
 * usage stats for each node, which will be returned in the event of a failure response from that node.
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

    private final Map<String, NodeUsageStatsForThreadPools> lastNodeUsageStatsPerNode = new ConcurrentHashMap<>();

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
        var dataNodeIds = clusterState.nodes().getDataNodes().values().stream().map(DiscoveryNode::getId).toArray(String[]::new);
        // Discard last-seen values for any nodes no longer present in the cluster state
        lastNodeUsageStatsPerNode.keySet().retainAll(Arrays.asList(dataNodeIds));
        if (clusterState.getMinTransportVersion().supports(TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION)) {
            client.execute(
                TransportNodeUsageStatsForThreadPoolsAction.TYPE,
                new NodeUsageStatsForThreadPoolsAction.Request(dataNodeIds),
                listener.map(this::replaceFailuresWithLastSeenValues)
            );
        } else {
            listener.onResponse(Map.of());
        }
    }

    private Map<String, NodeUsageStatsForThreadPools> replaceFailuresWithLastSeenValues(
        NodeUsageStatsForThreadPoolsAction.Response response
    ) {
        final Map<String, NodeUsageStatsForThreadPools> returnedUsageStats = response.getAllNodeUsageStatsForThreadPools();
        // Update the last-seen usage stats
        this.lastNodeUsageStatsPerNode.putAll(returnedUsageStats);

        if (response.hasFailures() == false) {
            return returnedUsageStats;
        }

        // Add in the last-seen usage stats for any nodes that failed to respond
        final Map<String, NodeUsageStatsForThreadPools> cachedValuesForFailed = new HashMap<>(returnedUsageStats);
        for (FailedNodeException failedNodeException : response.failures()) {
            final var nodeUsageStatsForThreadPools = lastNodeUsageStatsPerNode.get(failedNodeException.nodeId());
            if (nodeUsageStatsForThreadPools != null) {
                cachedValuesForFailed.put(failedNodeException.nodeId(), nodeUsageStatsForThreadPools);
            }
        }
        return cachedValuesForFailed;
    }
}
