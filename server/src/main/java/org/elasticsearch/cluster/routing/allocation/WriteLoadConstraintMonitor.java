/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Monitors the node-level write thread pool usage across the cluster and initiates a rebalancing round (via
 * {@link RerouteService#reroute}) whenever a node crosses the node-level write load thresholds.
 */
public class WriteLoadConstraintMonitor {
    private static final Logger logger = LogManager.getLogger(WriteLoadConstraintMonitor.class);
    private static final int MAX_NODE_IDS_IN_MESSAGE = 3;
    private final WriteLoadConstraintSettings writeLoadConstraintSettings;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private volatile long lastRerouteTimeMillis = 0;
    private volatile Set<String> lastSetOfHotSpottedNodes = Set.of();

    public WriteLoadConstraintMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        this.writeLoadConstraintSettings = new WriteLoadConstraintSettings(clusterSettings);
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;
    }

    /**
     * Receives a copy of the latest {@link ClusterInfo} whenever the {@link ClusterInfoService} collects it. Processes the new
     * {@link org.elasticsearch.cluster.NodeUsageStatsForThreadPools} and initiates rebalancing, via reroute, if a node in the cluster
     * exceeds thread pool usage thresholds.
     */
    public void onNewInfo(ClusterInfo clusterInfo) {
        final ClusterState state = clusterStateSupplier.get();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            logger.debug("skipping monitor because the write load decider is not fully enabled");
            return;
        }

        logger.trace("processing new cluster info");

        final int numberOfNodes = clusterInfo.getNodeUsageStatsForThreadPools().size();
        final Set<String> nodeIdsExceedingLatencyThreshold = Sets.newHashSetWithExpectedSize(numberOfNodes);
        final Set<String> potentialRelocationTargets = Sets.newHashSetWithExpectedSize(numberOfNodes);
        clusterInfo.getNodeUsageStatsForThreadPools().forEach((nodeId, usageStats) -> {
            final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = usageStats.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            assert writeThreadPoolStats != null : "Write thread pool is not publishing usage stats for node [" + nodeId + "]";
            if (writeThreadPoolStats.maxThreadPoolQueueLatencyMillis() > writeLoadConstraintSettings.getQueueLatencyThreshold().millis()) {
                nodeIdsExceedingLatencyThreshold.add(nodeId);
            } else if (writeThreadPoolStats.averageThreadPoolUtilization() <= writeLoadConstraintSettings.getHighUtilizationThreshold()) {
                potentialRelocationTargets.add(nodeId);
            }
        });
        assert Sets.intersection(nodeIdsExceedingLatencyThreshold, potentialRelocationTargets).isEmpty()
            : "We assume any nodes exceeding the latency threshold are not viable targets for relocation";

        if (nodeIdsExceedingLatencyThreshold.isEmpty()) {
            logger.debug("No hot-spotting nodes detected");
            return;
        }

        // Remove any over-threshold nodes that already have shards relocating away
        final RoutingNodes routingNodes = state.getRoutingNodes();
        nodeIdsExceedingLatencyThreshold.removeIf(
            nodeId -> routingNodes.node(nodeId).numberOfShardsWithState(ShardRoutingState.RELOCATING) > 0
        );

        if (nodeIdsExceedingLatencyThreshold.isEmpty()) {
            logger.debug("All nodes over threshold have relocation in progress");
            return;
        }

        if (potentialRelocationTargets.isEmpty()) {
            logger.debug("No nodes are suitable as relocation targets");
            return;
        }

        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        final long timeSinceLastRerouteMillis = currentTimeMillis - lastRerouteTimeMillis;
        final boolean haveCalledRerouteRecently = timeSinceLastRerouteMillis < writeLoadConstraintSettings.getMinimumRerouteInterval()
            .millis();

        if (haveCalledRerouteRecently == false
            || Sets.difference(nodeIdsExceedingLatencyThreshold, lastSetOfHotSpottedNodes).isEmpty() == false) {
            final String reason = Strings.format(
                "write load constraint monitor: "
                    + "Found %s exceeding the write thread pool queue latency threshold (%s with capacity, %d total)",
                nodeSummary(nodeIdsExceedingLatencyThreshold),
                nodeSummary(potentialRelocationTargets),
                state.nodes().size()
            );
            rerouteService.reroute(
                reason,
                Priority.NORMAL,
                ActionListener.wrap(
                    ignored -> logger.trace("{} reroute successful", reason),
                    e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
                )
            );
            lastRerouteTimeMillis = currentTimeMillisSupplier.getAsLong();
            lastSetOfHotSpottedNodes = nodeIdsExceedingLatencyThreshold;
        } else {
            logger.debug("Not calling reroute because we called reroute recently and there are no new hot spots");
        }
    }

    private static String nodeSummary(Set<String> nodeIds) {
        if (nodeIds.size() > 0 && nodeIds.size() <= MAX_NODE_IDS_IN_MESSAGE) {
            return "[" + String.join(", ", nodeIds) + "]";
        } else {
            return nodeIds.size() + " nodes";
        }
    }
}
