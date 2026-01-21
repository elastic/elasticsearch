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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Monitors the node-level write thread pool usage across the cluster and initiates a rebalancing round (via
 * {@link RerouteService#reroute}) whenever a node crosses the node-level write load thresholds.
 */
public class WriteLoadConstraintMonitor {
    public static final String HOTSPOT_NODES_COUNT_METRIC_NAME = "es.allocator.allocations.node.write_load_hotspot.current";
    public static final String HOTSPOT_DURATION_METRIC_NAME = "es.allocator.allocations.node.write_load_hotspot.duration.histogram";
    public static final String HOTSPOT_NODES_FLAG_METRIC_NAME = "es.allocator.allocations.node.write_load_hotspot_node_flag.current";

    private static final Logger logger = LogManager.getLogger(WriteLoadConstraintMonitor.class);
    private static final int MAX_NODE_IDS_IN_MESSAGE = 3;
    private final WriteLoadConstraintSettings writeLoadConstraintSettings;
    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;
    private volatile long lastRerouteTimeMillis = 0;
    private volatile Map<NodeIdName, Long> hotspotNodeStartTimes = Map.of();
    private long hotspotNodeStartTimesLastTerm = -1L;

    private final AtomicLong hotspotNodesCount = new AtomicLong(-1L); // metrics source of hotspotting node count
    private final DoubleHistogram hotspotDurationHistogram;
    private final LongGauge hotspotNodeFlagGauge; // nodes hotspotting have 1 written, with the name/id

    protected WriteLoadConstraintMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        // default of NOOP for tests
        this(clusterSettings, currentTimeMillisSupplier, clusterStateSupplier, rerouteService, MeterRegistry.NOOP);
    }

    public WriteLoadConstraintMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService,
        MeterRegistry meterRegistry
    ) {
        this.writeLoadConstraintSettings = new WriteLoadConstraintSettings(clusterSettings);
        this.clusterStateSupplier = clusterStateSupplier;
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.rerouteService = rerouteService;

        meterRegistry.registerLongsGauge(
            HOTSPOT_NODES_COUNT_METRIC_NAME,
            "Total number of nodes hotspotting with write loads",
            "unit",
            this::getHotspotNodesCount
        );
        hotspotDurationHistogram = meterRegistry.registerDoubleHistogram(HOTSPOT_DURATION_METRIC_NAME, "hotspot duration", "s");
        hotspotNodeFlagGauge = meterRegistry.registerLongsGauge(
            HOTSPOT_NODES_FLAG_METRIC_NAME,
            "hotspot node flag",
            "flag",
            this::getHotspottingNodeFlags
        );
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

        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        Set<String> lastHotspotNodes = recordHotspotDurations(state, writeNodeIdsExceedingQueueLatencyThreshold, currentTimeMillis);

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

        final long timeSinceLastRerouteMillis = currentTimeMillis - lastRerouteTimeMillis;
        final boolean haveCalledRerouteRecently = timeSinceLastRerouteMillis < writeLoadConstraintSettings.getMinimumRerouteInterval()
            .millis();

        // We know that there is at least one hot-spotting node if we've reached this code. Now check whether there are any new hot-spots
        // or hot-spots that are persisting and need further balancing work.
        Set<String> newHotspotNodes = Sets.difference(writeNodeIdsExceedingQueueLatencyThreshold, lastHotspotNodes);
        if (haveCalledRerouteRecently == false || newHotspotNodes.isEmpty() == false) {
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
                    nodeSummary(lastHotspotNodes),
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

            recordHotspotStartTimes(state, newHotspotNodes, currentTimeMillis);
        } else {
            logger.debug(
                "Not calling reroute because we called reroute [{}] ago and there are no new hot spots",
                TimeValue.timeValueMillis(timeSinceLastRerouteMillis)
            );
        }
    }

    private void recordHotspotStartTimes(ClusterState state, Set<String> nodeIds, long startTimestamp) {
        if (nodeIds.size() > 0) {
            Map<NodeIdName, Long> hotspotNodeStartTimesUpdate = new HashMap<>(hotspotNodeStartTimes);
            DiscoveryNodes nodes = state.nodes();
            for (String nodeId : nodeIds) {
                hotspotNodeStartTimesUpdate.put(NodeIdName.nodeIdName(nodes.get(nodeId)), startTimestamp);
            }
            hotspotNodeStartTimes = hotspotNodeStartTimesUpdate;
        }
        hotspotNodesCount.set(hotspotNodeStartTimes.size());
    }

    private Set<String> recordHotspotDurations(ClusterState state, Set<String> currentHotspotNodes, long hotspotEndTime) {
        // reset hotspotNodeStartTimes if the term has changed
        if (state.term() != hotspotNodeStartTimesLastTerm || state.nodes().isLocalNodeElectedMaster() == false) {
            hotspotNodeStartTimesLastTerm = state.term();
            hotspotNodeStartTimes = Map.of();
        }

        Set<String> lastHotspotNodes = hotspotNodeStartTimes.keySet()
            .stream()
            .map(nodeIdName -> nodeIdName.nodeId())
            .collect(Collectors.toSet());
        Set<String> staleHotspotNodes = Sets.difference(lastHotspotNodes, currentHotspotNodes);
        DiscoveryNodes nodes = state.nodes();
        if (staleHotspotNodes.size() > 0) {
            Map<NodeIdName, Long> hotspotNodeStartTimesUpdate = new HashMap<>(hotspotNodeStartTimes);
            for (String nodeId : staleHotspotNodes) {
                NodeIdName nodeIdName = NodeIdName.nodeIdName(nodes.get(nodeId));
                assert hotspotNodeStartTimesUpdate.containsKey(nodeIdName) : "Map should contain key from its own subset";
                long hotspotStartTime = hotspotNodeStartTimesUpdate.remove(nodeIdName);
                long hotspotDuration = hotspotEndTime - hotspotStartTime;
                assert hotspotDuration >= 0 : "hotspot duration should always be non-negative";
                hotspotDurationHistogram.record(
                    hotspotDuration / 1000.0,
                    Map.of("es_node_id", nodeIdName.nodeId(), "es_node_name", nodeIdName.nodeName())
                );
            }
            hotspotNodeStartTimes = hotspotNodeStartTimesUpdate;
        }

        hotspotNodesCount.set(hotspotNodeStartTimes.size());

        return lastHotspotNodes;
    }

    private List<LongWithAttributes> getHotspotNodesCount() {
        long hotspotCount = hotspotNodesCount.getAndSet(-1L);
        if (hotspotCount >= 0) {
            return List.of(new LongWithAttributes(hotspotCount));
        } else {
            return List.of();
        }
    }

    private Collection<LongWithAttributes> getHotspottingNodeFlags() {
        final Map<NodeIdName, Long> hotspotNodeStartTimesView = hotspotNodeStartTimes;
        List<LongWithAttributes> hotspottingNodeFlags = new ArrayList<>(hotspotNodeStartTimesView.size());
        for (NodeIdName nodeIdName : hotspotNodeStartTimesView.keySet()) {
            hotspottingNodeFlags.add(
                new LongWithAttributes(1L, Map.of("es_node_id", nodeIdName.nodeId(), "es_node_name", nodeIdName.nodeName()))
            );
        }
        return hotspottingNodeFlags;
    }

    public record NodeIdName(String nodeId, String nodeName) {
        public static NodeIdName nodeIdName(DiscoveryNode node) {
            return new NodeIdName(node.getId(), node.getName());
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
