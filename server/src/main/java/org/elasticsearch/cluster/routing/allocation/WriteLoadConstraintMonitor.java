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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
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
            logger.trace("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (writeLoadConstraintSettings.getWriteLoadConstraintEnabled().notFullyEnabled()) {
            logger.trace("skipping monitor because the write load decider is not fully enabled");
            return;
        }

        logger.trace("processing new cluster info");

        final int numberOfNodes = clusterInfo.getNodeUsageStatsForThreadPools().size();
        final Set<String> writeNodeIdsExceedingQueueLatencyThreshold = Sets.newHashSetWithExpectedSize(numberOfNodes);
        var haveWriteNodesBelowQueueLatencyThreshold = false;
        var totalIngestNodes = 0;
        for (var entry : clusterInfo.getNodeUsageStatsForThreadPools().entrySet()) {
            final var nodeId = entry.getKey();
            final var usageStats = entry.getValue();
            final var nodeRoles = state.getNodes().get(nodeId).getRoles();
            if (nodeRoles.contains(DiscoveryNodeRole.SEARCH_ROLE) || nodeRoles.contains(DiscoveryNodeRole.ML_ROLE)) {
                // Search & ML nodes are not expected to have write load hot-spots and are not considered for shard relocation.
                // TODO (ES-13314): consider stateful data tiers
                continue;
            }
            totalIngestNodes++;
            final NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolStats = usageStats.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE);
            assert writeThreadPoolStats != null : "Write thread pool is not publishing usage stats for node [" + nodeId + "]";
            if (writeThreadPoolStats.maxThreadPoolQueueLatencyMillis() >= writeLoadConstraintSettings.getQueueLatencyThreshold().millis()) {
                writeNodeIdsExceedingQueueLatencyThreshold.add(nodeId);
            } else {
                haveWriteNodesBelowQueueLatencyThreshold = true;
            }
        }

        if (writeNodeIdsExceedingQueueLatencyThreshold.isEmpty()) {
            logger.trace("No hot-spotting write nodes detected");
            return;
        }
        if (haveWriteNodesBelowQueueLatencyThreshold == false) {
            logger.debug("""
                Nodes [{}] are above the queue latency threshold, but there are no write nodes below the threshold. \
                Cannot rebalance shards.""", nodeSummary(writeNodeIdsExceedingQueueLatencyThreshold));
            return;
        }

        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        final long timeSinceLastRerouteMillis = currentTimeMillis - lastRerouteTimeMillis;
        final boolean haveCalledRerouteRecently = timeSinceLastRerouteMillis < writeLoadConstraintSettings.getMinimumRerouteInterval()
            .millis();

        // We know that there is at least one hot-spotting node if we've reached this code. Now check whether there are any new hot-spots
        // or hot-spots that are persisting and need further balancing work.
        if (haveCalledRerouteRecently == false
            || Sets.difference(writeNodeIdsExceedingQueueLatencyThreshold, lastSetOfHotSpottedNodes).isEmpty() == false) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                    """
                        Nodes [{}] are hot-spotting, of {} total ingest nodes. Reroute for hot-spotting {}. \
                        Previously hot-spotting nodes are [{}]. The write thread pool queue latency threshold is [{}]. Triggering reroute.
                        """,
                    nodeSummary(writeNodeIdsExceedingQueueLatencyThreshold),
                    totalIngestNodes,
                    lastRerouteTimeMillis == 0
                        ? "has never previously been called"
                        : "was last called [" + TimeValue.timeValueMillis(timeSinceLastRerouteMillis) + "] ago",
                    nodeSummary(lastSetOfHotSpottedNodes),
                    writeLoadConstraintSettings.getQueueLatencyThreshold()
                );
            }
            final String reason = "hot-spotting detected by write load constraint monitor";
            rerouteService.reroute(
                reason,
                Priority.NORMAL,
                ActionListener.wrap(
                    ignored -> logger.trace("{} reroute successful", reason),
                    e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
                )
            );
            lastRerouteTimeMillis = currentTimeMillisSupplier.getAsLong();
            lastSetOfHotSpottedNodes = writeNodeIdsExceedingQueueLatencyThreshold;
        } else {
            logger.debug(
                "Not calling reroute because we called reroute [{}] ago and there are no new hot spots",
                TimeValue.timeValueMillis(timeSinceLastRerouteMillis)
            );
        }
    }

    private static String nodeSummary(Set<String> nodeIds) {
        if (nodeIds.isEmpty() == false && nodeIds.size() <= MAX_NODE_IDS_IN_MESSAGE) {
            return "[" + String.join(", ", nodeIds) + "]";
        } else {
            return nodeIds.size() + " nodes";
        }
    }
}
