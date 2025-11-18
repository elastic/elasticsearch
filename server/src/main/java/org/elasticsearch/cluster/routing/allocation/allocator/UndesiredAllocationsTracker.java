/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.ONE_MINUTE;
import static org.elasticsearch.core.TimeValue.ZERO;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;

/**
 * Keeps track of a limited number of shards that are currently in undesired allocations. If the
 * shards remain in undesired allocations for longer than a configurable threshold, it will log
 * why those shards can't be allocated to desired nodes.
 */
public class UndesiredAllocationsTracker {

    private static final Logger logger = LogManager.getLogger(UndesiredAllocationsTracker.class);

    private static final TimeValue FIVE_MINUTES = timeValueMinutes(5);
    private static final FeatureFlag UNDESIRED_ALLOCATION_TRACKER_ENABLED = new FeatureFlag("undesired_allocation_tracker");

    /**
     * Warning logs will be periodically written if we see a shard that's been in an undesired allocation for this long
     */
    public static final Setting<TimeValue> UNDESIRED_ALLOCATION_DURATION_LOG_THRESHOLD_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.threshold",
        FIVE_MINUTES,
        ONE_MINUTE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The minimum amount of time between warnings about persistent undesired allocations
     */
    public static final Setting<TimeValue> UNDESIRED_ALLOCATION_DURATION_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.interval",
        FIVE_MINUTES,
        ONE_MINUTE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The maximum number of undesired allocations to track. We expect this to be relatively small.
     */
    public static final Setting<Integer> MAX_UNDESIRED_ALLOCATIONS_TO_TRACK = Setting.intSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.max_to_track",
        UNDESIRED_ALLOCATION_TRACKER_ENABLED.isEnabled() ? 10 : 0,
        0,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TimeProvider timeProvider;
    private final LinkedHashMap<String, UndesiredAllocation> undesiredAllocations = new LinkedHashMap<>();
    private final FrequencyCappedAction undesiredAllocationDurationLogInterval;
    private volatile TimeValue undesiredAllocationDurationLoggingThreshold;
    private volatile int maxUndesiredAllocationsToTrack;
    private boolean missingAllocationAssertionsEnabled = true;

    UndesiredAllocationsTracker(ClusterSettings clusterSettings, TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.undesiredAllocationDurationLogInterval = new FrequencyCappedAction(timeProvider::relativeTimeInMillis, ZERO);
        clusterSettings.initializeAndWatch(
            UNDESIRED_ALLOCATION_DURATION_LOG_INTERVAL_SETTING,
            undesiredAllocationDurationLogInterval::setMinInterval
        );
        clusterSettings.initializeAndWatch(
            UNDESIRED_ALLOCATION_DURATION_LOG_THRESHOLD_SETTING,
            value -> undesiredAllocationDurationLoggingThreshold = value
        );
        clusterSettings.initializeAndWatch(MAX_UNDESIRED_ALLOCATIONS_TO_TRACK, value -> this.maxUndesiredAllocationsToTrack = value);
    }

    /**
     * Track an allocation as being undesired
     */
    public void trackUndesiredAllocation(ShardRouting shardRouting) {
        assert shardRouting.unassigned() == false : "Shouldn't record unassigned shards as undesired allocations";
        if (undesiredAllocations.size() < maxUndesiredAllocationsToTrack) {
            final var allocationId = shardRouting.allocationId().getId();
            if (undesiredAllocations.containsKey(allocationId) == false) {
                undesiredAllocations.put(
                    allocationId,
                    new UndesiredAllocation(shardRouting.shardId(), timeProvider.relativeTimeInMillis())
                );
            }
        }
    }

    /**
     * Remove any tracking of the specified allocation (a no-op if the allocation isn't being tracked)
     */
    public void removeTracking(ShardRouting shardRouting) {
        assert shardRouting.unassigned() == false : "Shouldn't remove tracking of unassigned shards";
        undesiredAllocations.remove(shardRouting.allocationId().getId());
    }

    /**
     * Clear any {@link ShardRouting} that are no longer present in the routing nodes
     */
    public void cleanup(RoutingNodes routingNodes) {
        undesiredAllocations.entrySet().removeIf(e -> {
            final var undesiredAllocation = e.getValue();
            final var allocationId = e.getKey();
            return routingNodes.getByAllocationId(undesiredAllocation.shardId(), allocationId) == null;
        });
        shrinkIfOversized();
    }

    /**
     * Clear all tracked allocations
     */
    public void clear() {
        undesiredAllocations.clear();
    }

    /**
     * If there are shards that have been in undesired allocations for longer than the configured
     * threshold, log a warning
     */
    public void maybeLogUndesiredShardsWarning(
        RoutingNodes routingNodes,
        RoutingAllocation routingAllocation,
        DesiredBalance desiredBalance
    ) {
        final long currentTimeMillis = timeProvider.relativeTimeInMillis();
        if (undesiredAllocations.isEmpty() == false) {
            final long earliestUndesiredTimestamp = undesiredAllocations.firstEntry().getValue().undesiredSince();
            if (earliestUndesiredTimestamp < currentTimeMillis
                && currentTimeMillis - earliestUndesiredTimestamp > undesiredAllocationDurationLoggingThreshold.millis()) {
                undesiredAllocationDurationLogInterval.maybeExecute(
                    () -> logDecisionsForUndesiredShardsOverThreshold(routingNodes, routingAllocation, desiredBalance)
                );
            }
        }
    }

    private boolean shardTierMatchesNodeTier(ShardRouting shardRouting, DiscoveryNode discoveryNode) {
        return switch (shardRouting.role()) {
            case INDEX_ONLY -> discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
            case SEARCH_ONLY -> discoveryNode.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
            default -> true;
        };
    }

    private void logDecisionsForUndesiredShardsOverThreshold(
        RoutingNodes routingNodes,
        RoutingAllocation routingAllocation,
        DesiredBalance desiredBalance
    ) {
        final long currentTimeMillis = timeProvider.relativeTimeInMillis();
        final long loggingThresholdTimestamp = currentTimeMillis - undesiredAllocationDurationLoggingThreshold.millis();
        for (var allocation : undesiredAllocations.entrySet()) {
            final var undesiredAllocation = allocation.getValue();
            final var allocationId = allocation.getKey();
            if (undesiredAllocation.undesiredSince() < loggingThresholdTimestamp) {
                final var shardRouting = routingNodes.getByAllocationId(undesiredAllocation.shardId(), allocationId);
                if (shardRouting != null) {
                    logUndesiredShardDetails(
                        shardRouting,
                        timeValueMillis(currentTimeMillis - undesiredAllocation.undesiredSince()),
                        routingNodes,
                        routingAllocation,
                        desiredBalance
                    );
                } else {
                    assert false : undesiredAllocation + " for allocationID " + allocationId + " was not cleaned up";
                }
            }
        }
    }

    private void logUndesiredShardDetails(
        ShardRouting shardRouting,
        TimeValue undesiredDuration,
        RoutingNodes routingNodes,
        RoutingAllocation allocation,
        DesiredBalance desiredBalance
    ) {
        final RoutingAllocation.DebugMode originalDebugMode = allocation.getDebugMode();
        allocation.setDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);
        try {
            final var assignment = desiredBalance.getAssignment(shardRouting.shardId());
            if (assignment != null) {
                logger.warn("Shard {} has been in an undesired allocation for {}", shardRouting.shardId(), undesiredDuration);
                for (final var nodeId : assignment.nodeIds()) {
                    if (allocation.nodes().nodeExists(nodeId)) {
                        if (shardTierMatchesNodeTier(shardRouting, allocation.nodes().get(nodeId))) {
                            final var decision = allocation.deciders().canAllocate(shardRouting, routingNodes.node(nodeId), allocation);
                            logger.warn("Shard {} allocation decision for node [{}]: {}", shardRouting.shardId(), nodeId, decision);
                        }
                    } else {
                        logger.warn("Shard {} desired node [{}] has left the cluster", shardRouting.shardId(), nodeId);
                    }
                }
            } else {
                assert missingAllocationAssertionsEnabled == false
                    : "Shard " + shardRouting + " was missing an assignment, this shouldn't be possible. " + desiredBalance;
            }
        } finally {
            allocation.setDebugMode(originalDebugMode);
        }
    }

    /**
     * If the maximum to track was reduced, and we are tracking more than the new maximum, purge the most recent entries
     * to bring us under the new limit
     */
    private void shrinkIfOversized() {
        if (undesiredAllocations.size() > maxUndesiredAllocationsToTrack) {
            final var newestExcessAllocationIds = undesiredAllocations.entrySet()
                .stream()
                .sorted((a, b) -> Long.compare(b.getValue().undesiredSince(), a.getValue().undesiredSince()))
                .limit(undesiredAllocations.size() - maxUndesiredAllocationsToTrack)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
            undesiredAllocations.keySet().removeAll(newestExcessAllocationIds);
        }
    }

    // visible for testing
    Map<String, UndesiredAllocation> getUndesiredAllocations() {
        return Map.copyOf(undesiredAllocations);
    }

    /**
     * Rather than storing the {@link ShardRouting}, we store a map of allocationId -> {@link UndesiredAllocation}
     * this is because the allocation ID will persist as long as a shard stays on the same node, but the
     * {@link ShardRouting} changes for a variety of reasons even when the shard doesn't move.
     *
     * @param shardId The shard ID
     * @param undesiredSince The timestamp when the shard was first observed in an undesired allocation
     */
    record UndesiredAllocation(ShardId shardId, long undesiredSince) {}

    // Exposed for testing
    public Releasable disableMissingAllocationAssertions() {
        missingAllocationAssertionsEnabled = false;
        return () -> missingAllocationAssertionsEnabled = true;
    }
}
