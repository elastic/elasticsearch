/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.FrequencyCappedAction;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Keeps track of a limited number of shards that are currently in undesired allocations. If the
 * shards remain in undesired allocations for longer than a configurable threshold, it will log
 * why those shards can't be allocated to desired nodes.
 */
public class UndesiredAllocationsTracker {

    private static final Logger logger = LogManager.getLogger(UndesiredAllocationsTracker.class);

    /**
     * Warning logs will be periodically written if we see a shard that's been in an undesired allocation for this long
     */
    public static final Setting<TimeValue> UNDESIRED_ALLOCATION_DURATION_LOG_THRESHOLD_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.threshold",
        TimeValue.timeValueMinutes(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The minimum amount of time between warnings about persistent undesired allocations
     */
    public static final Setting<TimeValue> UNDESIRED_ALLOCATION_DURATION_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMinutes(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The max number of undesired allocations to track. We expect this to be relatively small.
     */
    public static final Setting<Integer> MAX_UNDESIRED_ALLOCATIONS_TO_TRACK = Setting.intSetting(
        "cluster.routing.allocation.desired_balance.undesired_duration_logging.max_to_track",
        0,
        0,
        200,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final TimeProvider timeProvider;
    private final ObjectLongHashMap<ShardRouting> undesiredAllocations = new ObjectLongHashMap<>();
    private final FrequencyCappedAction undesiredAllocationDurationLogInterval;
    private volatile TimeValue undesiredAllocationDurationLoggingThreshold;
    private volatile int maxUndesiredAllocationsToTrack;

    UndesiredAllocationsTracker(ClusterSettings clusterSettings, TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        this.undesiredAllocationDurationLogInterval = new FrequencyCappedAction(timeProvider::relativeTimeInMillis, TimeValue.ZERO);
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
        if (undesiredAllocations.size() < maxUndesiredAllocationsToTrack && undesiredAllocations.containsKey(shardRouting) == false) {
            undesiredAllocations.put(shardRouting, timeProvider.relativeTimeInMillis());
        }
    }

    /**
     * Remove any tracking of the specified allocation (a no-op if the allocation isn't being tracked)
     */
    public void removeTracking(ShardRouting shardRouting) {
        undesiredAllocations.remove(shardRouting);
    }

    /**
     * Clear any {@link ShardRouting} that are no longer present in the routing nodes
     */
    public void cleanup(RoutingNodes routingNodes) {
        undesiredAllocations.removeAll(shardRouting -> {
            final var allocationId = shardRouting.allocationId();
            if (allocationId != null) {
                return routingNodes.getByAllocationId(shardRouting.shardId(), allocationId.getId()) == null;
            } else {
                assert false : "Unassigned shards shouldn't be marked as undesired";
                return true;
            }
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
        long earliestUndesiredTimestamp = Long.MAX_VALUE;
        for (var allocation : undesiredAllocations) {
            if (allocation.value < earliestUndesiredTimestamp) {
                earliestUndesiredTimestamp = allocation.value;
            }
        }
        if (earliestUndesiredTimestamp < currentTimeMillis
            && currentTimeMillis - earliestUndesiredTimestamp > undesiredAllocationDurationLoggingThreshold.millis()) {
            undesiredAllocationDurationLogInterval.maybeExecute(
                () -> logDecisionsForUndesiredShardsOverThreshold(routingNodes, routingAllocation, desiredBalance)
            );
        }
    }

    private void logDecisionsForUndesiredShardsOverThreshold(
        RoutingNodes routingNodes,
        RoutingAllocation routingAllocation,
        DesiredBalance desiredBalance
    ) {
        final long currentTimeMillis = timeProvider.relativeTimeInMillis();
        final long loggingThresholdTimestamp = currentTimeMillis - undesiredAllocationDurationLoggingThreshold.millis();
        for (var allocation : undesiredAllocations) {
            if (allocation.value < loggingThresholdTimestamp) {
                logUndesiredShardDetails(
                    allocation.key,
                    TimeValue.timeValueMillis(currentTimeMillis - allocation.value),
                    routingNodes,
                    routingAllocation,
                    desiredBalance
                );
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
            logger.warn("Shard {} has been in an undesired allocation for {}", shardRouting.shardId(), undesiredDuration);
            for (final var nodeId : assignment.nodeIds()) {
                final var decision = allocation.deciders().canAllocate(shardRouting, routingNodes.node(nodeId), allocation);
                logger.warn("Shard {} allocation decision for node [{}]: {}", shardRouting.shardId(), nodeId, decision);
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
            final var newestExcessValues = StreamSupport.stream(undesiredAllocations.spliterator(), false)
                // we need to take a copy from the cursor because the cursors are re-used, so don't work with #sorted
                .map(cursor -> new Tuple<>(cursor.key, cursor.value))
                .sorted((a, b) -> Long.compare(b.v2(), a.v2()))
                .limit(undesiredAllocations.size() - maxUndesiredAllocationsToTrack)
                .map(Tuple::v1)
                .collect(Collectors.toSet());
            undesiredAllocations.removeAll(newestExcessValues::contains);
        }
    }

    // visible for testing
    ObjectLongMap<ShardRouting> getUndesiredAllocations() {
        return undesiredAllocations.clone();
    }
}
