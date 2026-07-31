/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Watches the per-node shared cache commitments recorded in {@link ClusterInfo} and triggers a reroute, via
 * {@link RerouteService#reroute}, when a search node's cache commitment newly crosses the high watermark or newly drops back below the
 * low watermark. {@link SharedCacheCapacityAllocationDecider} is only consulted while a reroute is already in progress, but a node's
 * cache commitment can drift past a watermark on its own, for example as shards grow, as boost configuration changes, or as data ages
 * out of the boost window. This monitor is what notices that drift and asks for a reroute so the decider gets a chance to act on it.
 */
public class SharedCacheCapacityMonitor {

    private static final Logger logger = LogManager.getLogger(SharedCacheCapacityMonitor.class);

    private final Supplier<ClusterState> clusterStateSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final RerouteService rerouteService;

    private volatile boolean enabled;
    private volatile SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode;
    private volatile RatioValue lowWatermark;
    private volatile RatioValue highWatermark;
    private volatile TimeValue minimumRerouteInterval;

    private final Object mutex = new Object();
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
     * Compares each search node's cache commitment, from {@link ClusterInfo#getNodeCacheSizeAndCommitments()}, against the commitment
     * recorded on the previous call and triggers a reroute when a node newly crosses the high watermark or newly drops back below the
     * low watermark.
     */
    public void onNewInfo(ClusterInfo clusterInfo) {
        if (clusterStateSupplier.get().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            logger.debug("skipping monitor as the cluster state is not recovered yet");
            return;
        }

        if (enabled == false) {
            logger.debug("skipping monitor as the shared cache capacity decider is disabled");
            synchronized (mutex) {
                lastNodeCommitments = Map.of();
            }
            return;
        }

        final ClusterState state = clusterStateSupplier.get();
        final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = clusterInfo.getNodeCacheSizeAndCommitments();

        // Restrict the current-call snapshot to search nodes present in the cluster state right now, so a node that has left the
        // cluster is naturally excluded from the next comparison rather than being mistaken for a node whose commitment dropped.
        // Cluster state and cluster info can disagree transiently while a node joins or drops out, so a node with no recorded
        // commitments is skipped rather than treated as having zero commitment.
        final Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE))
            .filter(node -> nodeCacheSizeAndCommitments.containsKey(node.getId()))
            .collect(Collectors.toUnmodifiableMap(node -> node, node -> nodeCacheSizeAndCommitments.get(node.getId())));

        final RerouteDecision rerouteDecision;
        synchronized (mutex) {
            final Map<DiscoveryNode, NodeCacheSizeAndCommitments> previousSearchNodeCommitments = lastNodeCommitments;
            lastNodeCommitments = currentSearchNodeCommitments;

            final RerouteDecision candidateDecision = decideReroute(currentSearchNodeCommitments, previousSearchNodeCommitments);

            // Snapshot the clock right before it's used, so successive calls compare against a consistent reading. Every reroute
            // reason here reflects a transition observed on this call, so unlike a monitor that re-reports a persisting condition on
            // every tick, there's no "already known, skip it" case to bypass: the interval is a plain floor between reroutes.
            final long currentTimeMillis = currentTimeMillisSupplier.getAsLong();
            final boolean haveCalledRerouteRecently = (currentTimeMillis - lastRerouteTimeMillis) < minimumRerouteInterval.millis();

            if (candidateDecision.shouldReroute() && haveCalledRerouteRecently == false) {
                rerouteDecision = candidateDecision;
                lastRerouteTimeMillis = currentTimeMillis;
            } else {
                rerouteDecision = RerouteDecision.no(candidateDecision.transitions());
            }
        }

        if (rerouteDecision.shouldReroute()) {
            reroute(rerouteDecision.reason());
        }
    }

    /**
     * Whether a reroute is warranted and, if so, why, plus the {@link NodeWatermarkTransitions} the decision was based on.
     */
    record RerouteDecision(boolean shouldReroute, String reason, NodeWatermarkTransitions transitions) {

        private static RerouteDecision no(NodeWatermarkTransitions transitions) {
            return new RerouteDecision(false, null, transitions);
        }

        private static RerouteDecision yes(String reason, NodeWatermarkTransitions transitions) {
            return new RerouteDecision(true, reason, transitions);
        }
    }

    /**
     * Compares each search node's current cache commitment against its commitment on the previous call and decides whether a reroute
     * is warranted. Nodes that have no prior commitment to compare against (for example, newly observed nodes) are treated as having
     * been below both watermarks, so they can still be reported as newly exceeding the high watermark. Nodes that leave the cluster
     * between calls are naturally absent from the comparison and are never treated as having "dropped". An over-subscribed node, one
     * that newly crosses the high watermark, is the more urgent condition, so it is checked first. The low watermark is checked only
     * when the high watermark gives no reason to reroute.
     */
    RerouteDecision decideReroute(
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> previousSearchNodeCommitments
    ) {
        final NodeWatermarkTransitions transitions = classifyWatermarkTransitions(
            currentSearchNodeCommitments,
            previousSearchNodeCommitments
        );

        if (transitions.nodesNewlyExceedingHighWatermark().size() > 0) {
            if (transitions.nodesBelowLowWatermark().isEmpty()) {
                // Every search node is already over the low watermark, so there is nowhere better to move shards to. Rerouting would
                // not relieve the pressure, so we skip it and wait for the cluster to be scaled instead.
                logger.debug(
                    "not rerouting for nodes {} newly exceeding the high watermark because all search nodes exceed the low watermark",
                    shortDescriptions(transitions.nodesNewlyExceedingHighWatermark())
                );
            } else {
                logger.debug(
                    "cache commitments exceeded the high watermark for nodes {}, triggering reroute",
                    shortDescriptions(transitions.nodesNewlyExceedingHighWatermark())
                );
                return RerouteDecision.yes("shared cache capacity exceeded high watermark", transitions);
            }
        }

        if (transitions.nodesNewlyDroppedBelowLowWatermark().size() > 0) {
            logger.debug(
                "cache commitments dropped below the low watermark for nodes {}, triggering reroute",
                shortDescriptions(transitions.nodesNewlyDroppedBelowLowWatermark())
            );
            return RerouteDecision.yes("shared cache capacity dropped below low watermark", transitions);
        }

        return RerouteDecision.no(transitions);
    }

    private static Set<String> shortDescriptions(Set<DiscoveryNode> nodes) {
        return nodes.stream().map(DiscoveryNode::getShortNodeDescription).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * The per-node watermark transitions observed between the previous call and this one, plus the current search nodes below the low
     * watermark right now.
     */
    record NodeWatermarkTransitions(
        Set<DiscoveryNode> nodesNewlyExceedingHighWatermark,
        Set<DiscoveryNode> nodesNewlyDroppedBelowLowWatermark,
        Set<DiscoveryNode> nodesBelowLowWatermark
    ) {}

    /**
     * Classifies each search node's current cache commitment against its commitment on the previous call. A node present in only one
     * of the two maps, because it joined or left the cluster between calls, is not compared at all. A node that has no prior
     * commitment to compare against is treated as not having exceeded either watermark before, so a node that starts out over the
     * high watermark is still reported as newly exceeding it, but a brand new node can never be reported as having dropped below a
     * watermark it was never compared against.
     */
    private NodeWatermarkTransitions classifyWatermarkTransitions(
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> currentSearchNodeCommitments,
        Map<DiscoveryNode, NodeCacheSizeAndCommitments> previousSearchNodeCommitments
    ) {
        // Snapshot the watermark settings once before classifying nodes, so a concurrent settings update can't apply different
        // watermarks to different nodes within the same decision.
        final SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode = this.accountingMode;
        final RatioValue lowWatermark = this.lowWatermark;
        final RatioValue highWatermark = this.highWatermark;

        final Set<DiscoveryNode> nodesNewlyExceedingHighWatermark = new HashSet<>();
        final Set<DiscoveryNode> nodesNewlyDroppedBelowLowWatermark = new HashSet<>();
        final Set<DiscoveryNode> nodesBelowLowWatermark = new HashSet<>();

        for (Map.Entry<DiscoveryNode, NodeCacheSizeAndCommitments> entry : currentSearchNodeCommitments.entrySet()) {
            final DiscoveryNode node = entry.getKey();
            final NodeCacheSizeAndCommitments current = entry.getValue();
            final long currentCommitmentBytes = accountingMode.getCurrentCommitmentBytes(current);
            final long lowWatermarkBytes = (long) (current.cacheSizeInBytes() * lowWatermark.getAsRatio());
            final long highWatermarkBytes = (long) (current.cacheSizeInBytes() * highWatermark.getAsRatio());
            final boolean exceedsLowWatermarkNow = currentCommitmentBytes > lowWatermarkBytes;
            final boolean exceedsHighWatermarkNow = currentCommitmentBytes > highWatermarkBytes;

            if (exceedsLowWatermarkNow == false) {
                nodesBelowLowWatermark.add(node);
            }

            final NodeCacheSizeAndCommitments previous = previousSearchNodeCommitments.get(node);
            if (previous == null) {
                if (exceedsHighWatermarkNow) {
                    nodesNewlyExceedingHighWatermark.add(node);
                }
                continue;
            }

            final long previousCommitmentBytes = accountingMode.getCurrentCommitmentBytes(previous);
            final boolean exceededLowWatermarkBefore = previousCommitmentBytes > lowWatermarkBytes;
            final boolean exceededHighWatermarkBefore = previousCommitmentBytes > highWatermarkBytes;

            if (exceedsHighWatermarkNow && exceededHighWatermarkBefore == false) {
                nodesNewlyExceedingHighWatermark.add(node);
            }
            if (exceedsLowWatermarkNow == false && exceededLowWatermarkBefore) {
                nodesNewlyDroppedBelowLowWatermark.add(node);
            }
        }

        return new NodeWatermarkTransitions(nodesNewlyExceedingHighWatermark, nodesNewlyDroppedBelowLowWatermark, nodesBelowLowWatermark);
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
