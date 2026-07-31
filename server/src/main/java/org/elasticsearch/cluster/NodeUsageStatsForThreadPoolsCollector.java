/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Collects the thread pool usage stats for each node in the cluster. The most recent values for each node are saved so that in the event
 * that a node fails to respond in a future request, the last received node stats can be returned.
 */
public class NodeUsageStatsForThreadPoolsCollector {

    /**
     * Holds the collected per-node thread pool usage stats and per-shard write load stats from a round of remote calls.
     */
    public record CollectedUsageStats(
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStats,
        Map<ShardId, Double> shardWriteLoadUtilizations
    ) {
        public static final CollectedUsageStats EMPTY = new CollectedUsageStats(Map.of(), Map.of());
    }

    public static final NodeUsageStatsForThreadPoolsCollector EMPTY = new NodeUsageStatsForThreadPoolsCollector() {
        public void collectUsageStats(Client client, ClusterState clusterState, ActionListener<CollectedUsageStats> listener) {
            listener.onResponse(CollectedUsageStats.EMPTY);
        }
    };

    private static final TransportVersion TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION = TransportVersion.fromName(
        "transport_node_usage_stats_for_thread_pools_action"
    );

    private static final Logger logger = LogManager.getLogger(NodeUsageStatsForThreadPoolsCollector.class);

    // These two maps save each node's last response to be used instead of returning empty node results in case a future node stats
    // collection call fails
    private final Map<String, NodeUsageStatsForThreadPools> lastNodeUsageStatsPerNode = new ConcurrentHashMap<>();
    private final Map<String, Map<ShardId, Double>> lastShardWriteLoadsPerNode = new ConcurrentHashMap<>();

    /**
     * Collects the thread pool usage stats ({@link NodeUsageStatsForThreadPools}) and per-shard write load utilizations
     * for each node in the cluster.
     *
     * @param listener The listener to receive the collected results.
     */
    public void collectUsageStats(Client client, ClusterState clusterState, ActionListener<CollectedUsageStats> listener) {
        var dataNodeIds = clusterState.nodes().getDataNodes().values().stream().map(DiscoveryNode::getId).toArray(String[]::new);
        // Discard last-seen values for any nodes no longer present in the cluster state
        lastNodeUsageStatsPerNode.keySet().retainAll(Arrays.asList(dataNodeIds));
        lastShardWriteLoadsPerNode.keySet().retainAll(Arrays.asList(dataNodeIds));
        if (clusterState.getMinTransportVersion().supports(TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION)) {
            client.execute(
                TransportNodeUsageStatsForThreadPoolsAction.TYPE,
                new NodeUsageStatsForThreadPoolsAction.Request(dataNodeIds),
                listener.map(response -> {
                    // Update last seen stats (failed nodes retain their previously cached values)
                    lastNodeUsageStatsPerNode.putAll(response.getAllNodeUsageStatsForThreadPools());
                    lastShardWriteLoadsPerNode.putAll(response.getAllShardWriteLoadUtilizationsPerNode());
                    if (response.failures().isEmpty() == false) {
                        logger.warn(
                            "Got no usage stats from nodes [{}], using last known stats for them",
                            response.failures().stream().map(FailedNodeException::nodeId).collect(Collectors.joining(", "))
                        );
                    }

                    return new CollectedUsageStats(
                        Map.copyOf(lastNodeUsageStatsPerNode),
                        Map.copyOf(convertShardUtilizationToThreadTime())
                    );
                })
            );
        } else {
            listener.onResponse(CollectedUsageStats.EMPTY);
        }
    }

    /**
     * {@link TransportNodeUsageStatsForThreadPoolsAction} returns shard write load values as the shard's % thread pool utilization during
     * the polling window. However, the total shard thread time is expected in the shard allocation code. Therefore, utilization will be
     * converted to thread time here before passing the values onward: see
     * {@link org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator#calculateUtilizationForWriteLoad} for details.
     */
    private Map<ShardId, Double> convertShardUtilizationToThreadTime() {
        final var allShardWriteThreadTime = new HashMap<ShardId, Double>();
        for (var nodeShardWriteLoads : lastShardWriteLoadsPerNode.entrySet()) {
            var threadPoolStats = lastNodeUsageStatsPerNode.get(nodeShardWriteLoads.getKey());
            assert threadPoolStats != null;
            var writeThreadPoolStats = threadPoolStats.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
            assert writeThreadPoolStats != null;
            var numThreads = writeThreadPoolStats.totalThreadPoolThreads();

            for (var shardWriteLoad : nodeShardWriteLoads.getValue().entrySet()) {
                allShardWriteThreadTime.put(shardWriteLoad.getKey(), shardWriteLoad.getValue() * numThreads);
            }
        }
        return allShardWriteThreadTime;
    }
}
