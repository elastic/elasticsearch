/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Notices when a search node's cache commitment crosses a watermark on its own, for example as shards grow or data ages out of the
 * boost window, and triggers a reroute via {@link RerouteService#reroute} so {@link SharedCacheCapacityAllocationDecider} gets a
 * chance to act on it. Only active when {@link SharedCacheCapacityAllocationDecider#CAN_REMAIN_ENABLED_SETTING} is also enabled, since
 * otherwise the decider's {@code canRemain} check would never see the reroute.
 *
 * <p>A reroute is suppressed when every over-committed node holds a shard whose requirement alone exceeds the high watermark of
 * every search node, since such a shard cannot be placed anywhere even on an empty node. This is a deliberate approximation: such a
 * node might still shed its other shards, but the largest shard is taken as the cause of the pressure. If shard requirement data is
 * absent from {@link ClusterInfo#getShardCacheRequirements()} (for example in a stateful cluster), the check fails open and the
 * reroute fires as before.
 */
public class SharedCacheCapacityMonitor {

    private static final Logger logger = LogManager.getLogger(SharedCacheCapacityMonitor.class);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;

    private volatile boolean enabled;
    private volatile boolean canRemainEnabled;
    private volatile SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode;
    private volatile RatioValue lowWatermark;
    private volatile RatioValue highWatermark;
    private volatile TimeValue minimumRerouteInterval;

    private Map<DiscoveryNode, NodeCacheSizeAndCommitments> lastNodeCommitments = Map.of();
    private long lastRerouteTimeMillis = 0;

    public SharedCacheCapacityMonitor(
        ClusterSettings clusterSettings,
        LongSupplier currentTimeMillisSupplier,
        Supplier<ClusterState> clusterStateSupplier,
        RerouteService rerouteService
    ) {
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.clusterStateSupplier = clusterStateSupplier;
        this.rerouteService = rerouteService;
        clusterSettings.initializeAndWatch(SharedCacheCapacityAllocationDecider.ENABLED_SETTING, value -> this.enabled = value);
        clusterSettings.initializeAndWatch(
            SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING,
            value -> this.canRemainEnabled = value
        );
        clusterSettings.initializeAndWatch(
            SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING,
            value -> this.accountingMode = value
        );
        clusterSettings.initializeAndWatch(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING, value -> this.lowWatermark = value);
        clusterSettings.initializeAndWatch(
            SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING,
            value -> this.highWatermark = value
        );
        clusterSettings.initializeAndWatch(
            SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING,
            value -> this.minimumRerouteInterval = value
        );
    }

    /**
     * Receives a copy of the latest {@link ClusterInfo} whenever the {@link org.elasticsearch.cluster.ClusterInfoService} collects it.
     * Compares each search node's cache commitment against the commitment recorded on the previous call and reroutes as decided by
     * {@link #decideReroute}. Only the retry case is throttled by {@link #minimumRerouteInterval}.
     */
    public void onNewInfo(ClusterInfo clusterInfo) {
        final ClusterState state = clusterStateSupplier.get();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (enabled == false || canRemainEnabled == false) {
            logger.debug("skipping monitor as the shared cache capacity decider or its canRemain check is disabled");
            lastNodeCommitments = Map.of();
            return;
        }

        final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = clusterInfo.getNodeCacheSizeAndCommitments();

        // Restrict the snapshot to search nodes present in the cluster state right now, so a departed node is excluded rather
        // than mistaken for one whose commitment dropped. A node missing from ClusterInfo is skipped rather than treated as
        // having zero commitment, since cluster state and cluster info can disagree transiently while a node joins or leaves.
        final Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .filter(node -> nodeCacheSizeAndCommitments.containsKey(node.getId()))
            .collect(Collectors.toUnmodifiableMap(node -> node, node -> nodeCacheSizeAndCommitments.get(node.getId())));

        // Snapshot the clock right before it's used, so successive calls compare against a consistent reading.
        final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
        final boolean intervalElapsed = (currentTimeMillis - lastRerouteTimeMillis) >= minimumRerouteInterval.millis();

        final RerouteDecision rerouteDecision = decideReroute(
            state.getRoutingNodes(),
            clusterInfo.getShardCacheRequirements(),
            currentSearchNodeCommitments,
            lastNodeCommitments,
            intervalElapsed
        );
        lastNodeCommitments = currentSearchNodeCommitments;

        if (rerouteDecision.shouldReroute()) {
            lastRerouteTimeMillis = currentTimeMillis;
            reroute(rerouteDecision.reason());
        }
    }

    /**
     * Whether a reroute is warranted, why, and the {@link NodeWatermarkTransitions} the decision was based on.
     */
    record RerouteDecision(boolean shouldReroute, String reason, NodeWatermarkTransitions transitions) {

        static final String EXCEEDED_HIGH_WATERMARK_REASON = "shared cache capacity exceeded high watermark";
        static final String NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON = "new nodes exceeded shared cache capacity high watermark";
        static final String DROPPED_BELOW_LOW_WATERMARK_REASON = "new nodes dropped below shared cache capacity low watermark";

        private static RerouteDecision no(NodeWatermarkTransitions transitions) {
            return new RerouteDecision(false, null, transitions);
        }

        private static RerouteDecision yes(String reason, NodeWatermarkTransitions transitions) {
            return new RerouteDecision(true, reason, transitions);
        }
    }

    /**
     * Decides whether a reroute is warranted, requiring some node to currently be over the high watermark. Otherwise, there is
     * nothing for a reroute to relieve. Given that, a node newly crossing the high watermark or newly dropping below the low
     * watermark triggers a reroute immediately, regardless of {@code intervalElapsed}. Otherwise, the over-subscription is
     * retried once {@code intervalElapsed} is {@code true}, since the earlier reroute may not have relieved the pressure.
     *
     * <p>A reroute is also suppressed if every over-committed node holds a started shard whose requirement alone exceeds the high
     * watermark of every search node. This check scans shards, so it is only evaluated once a reroute is otherwise warranted. Absent
     * shard requirement data causes this gate to fail open, preserving behavior in environments where
     * {@link ClusterInfo#getShardCacheRequirements()} returns an empty map.
     */
    RerouteDecision decideReroute(
        RoutingNodes routingNodes,
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> previousSearchNodeCommitments,
        boolean intervalElapsed
    ) {
        final SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode = this.accountingMode;
        final NodeWatermarkTransitions transitions = classifyWatermarkTransitions(
            accountingMode,
            currentSearchNodeCommitments,
            previousSearchNodeCommitments
        );

        if (transitions.nodesOverHighWatermark().isEmpty() == false) {
            if (transitions.nodesBelowLowWatermark().isEmpty()) {
                // Every search node is over the low watermark, so there is nowhere to move shards to.
                logger.debug(
                    "not rerouting for nodes {} over the high watermark because all search nodes exceed the low watermark",
                    shortDescriptions(transitions.nodesOverHighWatermark())
                );
            } else {
                final String reason;
                final String logMessage;
                final Set<DiscoveryNode> nodesToLog;
                if (transitions.nodesNewlyExceedingHighWatermark().isEmpty() == false) {
                    reason = RerouteDecision.NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON;
                    logMessage = "cache commitments exceeded the high watermark for nodes {}, triggering reroute";
                    nodesToLog = transitions.nodesNewlyExceedingHighWatermark();
                } else if (transitions.nodesNewlyDroppedBelowLowWatermark().isEmpty() == false) {
                    reason = RerouteDecision.DROPPED_BELOW_LOW_WATERMARK_REASON;
                    logMessage = "cache commitments dropped below the low watermark for nodes {}, triggering reroute";
                    nodesToLog = transitions.nodesNewlyDroppedBelowLowWatermark();
                } else if (intervalElapsed) {
                    reason = RerouteDecision.EXCEEDED_HIGH_WATERMARK_REASON;
                    logMessage = "cache commitments for nodes {} remain over the high watermark, retrying reroute";
                    nodesToLog = transitions.nodesOverHighWatermark();
                } else {
                    return RerouteDecision.no(transitions);
                }

                // Evaluated last because it scans the shards on every over-committed node, and is only needed once a reroute is
                // otherwise warranted.
                if (everyOverCommittedNodeHasUnplaceableShard(routingNodes, shardCacheRequirements, transitions, accountingMode)) {
                    logger.debug(
                        "not rerouting for nodes {} over the high watermark because each holds a shard that exceeds the high "
                            + "watermark of every search node on its own",
                        shortDescriptions(transitions.nodesOverHighWatermark())
                    );
                    return RerouteDecision.no(transitions);
                }

                logger.debug(logMessage, shortDescriptions(nodesToLog));
                return RerouteDecision.yes(reason, transitions);
            }
        }

        return RerouteDecision.no(transitions);
    }

    private static Set<String> shortDescriptions(Set<DiscoveryNode> nodes) {
        return nodes.stream().map(DiscoveryNode::getShortNodeDescription).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * The per-node watermark state observed on this call, plus the transitions since the previous call.
     * {@code nodesOverHighWatermark} is the full current set, a superset of {@code nodesNewlyExceedingHighWatermark}, so a node
     * over the high watermark since an earlier call is still recognized even when it is not a new transition this time.
     * {@code maxHighWatermarkBytes} is the largest high watermark threshold, in bytes, across all search nodes. A shard requiring more
     * than that cannot be placed on any node without pushing it over the high watermark, even if it were empty.
     */
    record NodeWatermarkTransitions(
        Set<DiscoveryNode> nodesOverHighWatermark,
        Set<DiscoveryNode> nodesNewlyExceedingHighWatermark,
        Set<DiscoveryNode> nodesNewlyDroppedBelowLowWatermark,
        Set<DiscoveryNode> nodesBelowLowWatermark,
        long maxHighWatermarkBytes
    ) {}

    /**
     * Classifies each search node's current cache commitment against its commitment on the previous call. A node present in only
     * one map, because it joined or left the cluster between calls, is not compared. A node with no prior commitment is treated as
     * newly crossing.
     */
    private NodeWatermarkTransitions classifyWatermarkTransitions(
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> previousSearchNodeCommitments
    ) {
        // Snapshot the watermark settings once before classifying nodes, so a concurrent settings update cannot apply different
        // watermarks to different nodes in the same decision.
        final RatioValue lowWatermark = this.lowWatermark;
        final RatioValue highWatermark = this.highWatermark;

        final Set<DiscoveryNode> nodesOverHighWatermark = new HashSet<>();
        final Set<DiscoveryNode> nodesNewlyExceedingHighWatermark = new HashSet<>();
        final Set<DiscoveryNode> nodesNewlyDroppedBelowLowWatermark = new HashSet<>();
        final Set<DiscoveryNode> nodesBelowLowWatermark = new HashSet<>();
        long maxHighWatermarkBytes = 0L;

        for (Map.Entry<DiscoveryNode, NodeCacheSizeAndCommitments> entry : currentSearchNodeCommitments.entrySet()) {
            final DiscoveryNode node = entry.getKey();
            final NodeCacheSizeAndCommitments current = entry.getValue();
            final long currentCommitmentBytes = accountingMode.getCurrentCommitmentBytes(current);
            final boolean exceedsLowWatermarkNow = current.exceedsWatermark(currentCommitmentBytes, lowWatermark);
            final boolean exceedsHighWatermarkNow = current.exceedsWatermark(currentCommitmentBytes, highWatermark);

            maxHighWatermarkBytes = Math.max(maxHighWatermarkBytes, current.watermarkBytes(highWatermark));
            if (exceedsLowWatermarkNow == false) {
                nodesBelowLowWatermark.add(node);
            } else if (exceedsHighWatermarkNow) {
                nodesOverHighWatermark.add(node);
            }

            final NodeCacheSizeAndCommitments previous = previousSearchNodeCommitments.get(node);
            if (previous == null) {
                if (exceedsHighWatermarkNow) {
                    nodesNewlyExceedingHighWatermark.add(node);
                }
                if (exceedsLowWatermarkNow == false) {
                    nodesNewlyDroppedBelowLowWatermark.add(node);
                }
                continue;
            }

            final long previousCommitmentBytes = accountingMode.getCurrentCommitmentBytes(previous);
            final boolean exceededLowWatermarkBefore = previous.exceedsWatermark(previousCommitmentBytes, lowWatermark);
            final boolean exceededHighWatermarkBefore = previous.exceedsWatermark(previousCommitmentBytes, highWatermark);

            if (exceedsHighWatermarkNow && exceededHighWatermarkBefore == false) {
                nodesNewlyExceedingHighWatermark.add(node);
            }
            if (exceedsLowWatermarkNow == false && exceededLowWatermarkBefore) {
                nodesNewlyDroppedBelowLowWatermark.add(node);
            }
        }

        return new NodeWatermarkTransitions(
            nodesOverHighWatermark,
            nodesNewlyExceedingHighWatermark,
            nodesNewlyDroppedBelowLowWatermark,
            nodesBelowLowWatermark,
            maxHighWatermarkBytes
        );
    }

    /**
     * Whether every over-committed node holds a started shard whose requirement alone exceeds the largest high watermark of any
     * search node, so the node's pressure is taken to be caused by a shard that cannot be placed anywhere. A single over-committed
     * node without such a shard means a reroute might relieve it, so the answer is {@code false}. A node with no routing entry, no
     * started shards, or no requirement data for its shards has no shard known to be unplaceable, so this fails open.
     */
    private static boolean everyOverCommittedNodeHasUnplaceableShard(
        RoutingNodes routingNodes,
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements,
        NodeWatermarkTransitions transitions,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode
    ) {
        for (DiscoveryNode node : transitions.nodesOverHighWatermark()) {
            final RoutingNode routingNode = routingNodes.node(node.getId());
            if (routingNode == null
                || hasShardExceeding(routingNode, shardCacheRequirements, accountingMode, transitions.maxHighWatermarkBytes()) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasShardExceeding(
        RoutingNode routingNode,
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        long thresholdBytes
    ) {
        for (ShardRouting shard : routingNode.started()) {
            final BoostedAndUnboostedCacheRequirements requirement = shardCacheRequirements.get(shard.shardId());
            if (requirement != null && accountingMode.getShardRequirementBytes(requirement) > thresholdBytes) {
                return true;
            }
        }
        return false;
    }

    private void reroute(String reason) {
        rerouteService.reroute(
            reason,
            Priority.NORMAL,
            ActionListener.wrap(
                ignored -> logger.trace("{} reroute successful", reason),
                e -> logger.debug(() -> Strings.format("reroute failed, reason: %s", reason), e)
            )
        );
    }
}
