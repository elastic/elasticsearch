/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Holds the desired balance and updates it as the cluster evolves.
 */
public class DesiredBalanceComputer {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceComputer.class);

    private final ShardsAllocator delegateAllocator;
    private final TimeProvider timeProvider;

    // stats
    protected final MeanMetric iterations = new MeanMetric();

    public static final Setting<TimeValue> PROGRESS_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.progress_log_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MAX_BALANCE_COMPUTATION_TIME_DURING_INDEX_CREATION_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.max_balance_computation_time_during_index_creation",
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue progressLogInterval;
    private long maxBalanceComputationTimeDuringIndexCreationMillis;
    private long numComputeCallsSinceLastConverged;
    private long numIterationsSinceLastConverged;
    private long lastConvergedTimeMillis;
    private long lastNotConvergedLogMessageTimeMillis;
    private Level convergenceLogMsgLevel;

    public DesiredBalanceComputer(ClusterSettings clusterSettings, TimeProvider timeProvider, ShardsAllocator delegateAllocator) {
        this.delegateAllocator = delegateAllocator;
        this.timeProvider = timeProvider;
        this.numComputeCallsSinceLastConverged = 0;
        this.numIterationsSinceLastConverged = 0;
        this.lastConvergedTimeMillis = timeProvider.relativeTimeInMillis();
        this.lastNotConvergedLogMessageTimeMillis = lastConvergedTimeMillis;
        this.convergenceLogMsgLevel = Level.DEBUG;
        clusterSettings.initializeAndWatch(PROGRESS_LOG_INTERVAL_SETTING, value -> this.progressLogInterval = value);
        clusterSettings.initializeAndWatch(
            MAX_BALANCE_COMPUTATION_TIME_DURING_INDEX_CREATION_SETTING,
            value -> this.maxBalanceComputationTimeDuringIndexCreationMillis = value.millis()
        );
    }

    public DesiredBalance compute(
        DesiredBalance previousDesiredBalance,
        DesiredBalanceInput desiredBalanceInput,
        Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
        Predicate<DesiredBalanceInput> isFresh
    ) {
        // TODO NOMERGE: can I find out which shards must move beforehand?
        // Ans: no, I don't think so.
        // Not all shards that cannot remain need to be moved off to satisfy disk threshold, for example.

        numComputeCallsSinceLastConverged += 1;
        if (logger.isTraceEnabled()) {
            logger.trace(
                "Recomputing desired balance for [{}]: {}, {}, {}, {}",
                desiredBalanceInput.index(),
                previousDesiredBalance,
                desiredBalanceInput.routingAllocation().routingNodes().toString(),
                desiredBalanceInput.routingAllocation().clusterInfo().toString(),
                desiredBalanceInput.routingAllocation().snapshotShardSizeInfo().toString()
            );
        } else {
            logger.debug("Recomputing desired balance for [{}]", desiredBalanceInput.index());
        }

        final var mutableRoutingAllocation = desiredBalanceInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = mutableRoutingAllocation.routingNodes();
        final var knownNodeIds = routingNodes.getAllNodeIds();
        final var changes = mutableRoutingAllocation.changes();
        final var ignoredShards = getIgnoredShardsWithDiscardedAllocationStatus(desiredBalanceInput.ignoredShards());
        final var clusterInfoSimulator = new ClusterInfoSimulator(mutableRoutingAllocation);
        DesiredBalance.ComputationFinishReason finishReason = DesiredBalance.ComputationFinishReason.CONVERGED;

        if (routingNodes.size() == 0) {
            return new DesiredBalance(desiredBalanceInput.index(), Map.of(), Map.of(), finishReason);
        }

        // Change any intializing shards to started, in our routing table copy.
        // We presume that all ongoing recoveries will complete.
        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                if (shardRouting.initializing()) {
                    clusterInfoSimulator.simulateShardStarted(shardRouting);
                    routingNodes.startShard(shardRouting, changes, 0L);
                }
            }
        }

        // we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
        // unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
        final var unassignedPrimaries = new HashSet<ShardId>();
        final var shardRoutings = new HashMap<ShardId, ShardRoutings>();
        for (final var primary : new boolean[] { true, false }) {
            final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            for (final var iterator = unassigned.iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.primary() == primary) {
                    var lastAllocatedNodeId = shardRouting.unassignedInfo().lastAllocatedNodeId();
                    if (knownNodeIds.contains(lastAllocatedNodeId)
                        || ignoredShards.contains(discardAllocationStatus(shardRouting)) == false) {
                        shardRoutings.computeIfAbsent(shardRouting.shardId(), ShardRoutings::new).unassigned().add(shardRouting);
                    } else {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                        if (shardRouting.primary()) {
                            unassignedPrimaries.add(shardRouting.shardId());
                        }
                    }
                }
            }
        }

        for (final var assigned : routingNodes.getAssignedShards().entrySet()) {
            shardRoutings.computeIfAbsent(assigned.getKey(), ShardRoutings::new).assigned().addAll(assigned.getValue());
        }

        // Go through each ShardId's unassigned and assigned shard copies
        // we can assume that all possible shards will be allocated/relocated to one of their desired locations
        final var unassignedShardsToInitialize = new HashMap<ShardRouting, LinkedList<String>>();
        for (final var entry : shardRoutings.entrySet()) {
            final var shardId = entry.getKey();
            final var routings = entry.getValue();

            // treemap (keyed by node ID) so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeMap<String, ShardRouting>();
            final var originalShardAssignment = previousDesiredBalance.getAssignment(shardId);

            // treeset (ordered by node ID) so that we are consistent about the order of future relocations
            final var targetNodes = originalShardAssignment != null
                ? new TreeSet<>(originalShardAssignment.nodeIds())
                : new TreeSet<String>();
            targetNodes.retainAll(knownNodeIds); // Filter out any nodes that have been removed from the cluster.

            // Avoid unnecessary relocations by preserving last known shard location as a starting assignment option.
            for (ShardRouting shardRouting : routings.unassigned()) {
                var lastAllocatedNodeId = shardRouting.unassignedInfo().lastAllocatedNodeId();
                // If the last attempt was on a node that is still part of the cluster, add the node as an eligible target.
                if (knownNodeIds.contains(lastAllocatedNodeId)) {
                    targetNodes.add(lastAllocatedNodeId);
                }
            }

            // Remove any node from targetNodes that already owns a shard copy, so we're left with targetNodes that can receive a copy.
            for (final var shardRouting : routings.assigned()) {
                assert shardRouting.started();
                if (targetNodes.remove(shardRouting.currentNodeId()) == false) {
                    final var previousShard = shardsToRelocate.put(shardRouting.currentNodeId(), shardRouting);
                    assert previousShard == null : "duplicate shards to relocate: " + shardRouting + " vs " + previousShard;
                }
            }

            final var targetNodesIterator = targetNodes.iterator();

            // Here existing shards are moved to desired locations before initializing unassigned shards because we prefer not to leave
            // immovable shards allocated to undesirable locations (e.g. a node that is shutting down or an allocation filter which was
            // only recently applied). In contrast, reconciliation prefers to initialize the unassigned shards first.
            relocateToDesiredLocation: for (final var shardRouting : shardsToRelocate.values()) {
                assert shardRouting.started();

                while (targetNodesIterator.hasNext()) {
                    final var targetNodeId = targetNodesIterator.next();
                    final var targetNode = routingNodes.node(targetNodeId);
                    if (targetNode != null
                        && mutableRoutingAllocation.deciders()
                            .canAllocate(shardRouting, targetNode, mutableRoutingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToRelocate = routingNodes.relocateShard(shardRouting, targetNodeId, 0L, "computation", changes).v2();
                        clusterInfoSimulator.simulateShardStarted(shardToRelocate);
                        routingNodes.startShard(shardToRelocate, changes, 0L);
                        continue relocateToDesiredLocation;
                    }
                }
            }

            for (final var shardRouting : routings.unassigned()) {
                assert shardRouting.unassigned();
                if (targetNodesIterator.hasNext()) {
                    unassignedShardsToInitialize.computeIfAbsent(shardRouting, ignored -> new LinkedList<>())
                        .add(targetNodesIterator.next());
                } else {
                    break;
                }
            }
        }

        final var unassignedPrimaryIterator = routingNodes.unassigned().iterator();
        while (unassignedPrimaryIterator.hasNext()) {
            final var shardRouting = unassignedPrimaryIterator.next();
            if (shardRouting.primary()) {
                final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
                if (nodeIds != null && nodeIds.isEmpty() == false) {
                    final var nodeId = nodeIds.removeFirst();
                    final var routingNode = routingNodes.node(nodeId);
                    if (routingNode != null
                        && mutableRoutingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, mutableRoutingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToInitialize = unassignedPrimaryIterator.initialize(nodeId, null, 0L, changes);
                        clusterInfoSimulator.simulateShardStarted(shardToInitialize);
                        routingNodes.startShard(shardToInitialize, changes, 0L);
                    }
                }
            }
        }

        final var unassignedReplicaIterator = routingNodes.unassigned().iterator();
        while (unassignedReplicaIterator.hasNext()) {
            final var shardRouting = unassignedReplicaIterator.next();
            if (unassignedPrimaries.contains(shardRouting.shardId()) == false) {
                final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
                if (nodeIds != null && nodeIds.isEmpty() == false) {
                    final var nodeId = nodeIds.removeFirst();
                    final var routingNode = routingNodes.node(nodeId);
                    if (routingNode != null
                        && mutableRoutingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, mutableRoutingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToInitialize = unassignedReplicaIterator.initialize(nodeId, null, 0L, changes);
                        clusterInfoSimulator.simulateShardStarted(shardToInitialize);
                        routingNodes.startShard(shardToInitialize, changes, 0L);
                    }
                }
            }
        }

        List<MoveAllocationCommand> commands;
        while ((commands = pendingDesiredBalanceMoves.poll()) != null) {
            for (MoveAllocationCommand command : commands) {
                try {
                    command.execute(mutableRoutingAllocation, false);
                } catch (RuntimeException e) {
                    logger.debug(
                        () -> "move shard ["
                            + command.index()
                            + ":"
                            + command.shardId()
                            + "] command failed during applying it to the desired balance",
                        e
                    );
                }
            }
        }

        final int iterationCountReportInterval = computeIterationCountReportInterval(mutableRoutingAllocation);
        final long timeWarningInterval = progressLogInterval.millis();
        final long computationStartedTime = timeProvider.relativeTimeInMillis();
        long nextReportTime = Math.max(lastNotConvergedLogMessageTimeMillis, lastConvergedTimeMillis) + timeWarningInterval;

        int i = 0;
        boolean hasChanges = false;
        boolean assignedNewlyCreatedPrimaryShards = false;
        while (true) {
            if (hasChanges) {
                // Not the first iteration, so every remaining unassigned shard has been ignored, perhaps due to throttling. We must bring
                // them all back out of the ignored list to give the allocator another go...
                routingNodes.unassigned().resetIgnored();

                // ... but not if they're ignored because they're out of scope for allocation
                for (final var iterator = routingNodes.unassigned().iterator(); iterator.hasNext();) {
                    final var shardRouting = iterator.next();
                    if (ignoredShards.contains(discardAllocationStatus(shardRouting))) {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                    }
                }
            }

            mutableRoutingAllocation.setSimulatedClusterInfo(clusterInfoSimulator.getClusterInfo());
            logger.trace("running delegate allocator");
            delegateAllocator.allocate(mutableRoutingAllocation);
            assert routingNodes.unassigned().isEmpty(); // any unassigned shards should now be ignored

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        if (shardRouting.primary()
                            && shardRouting.unassignedInfo() != null
                            && shardRouting.unassignedInfo().reason() == UnassignedInfo.Reason.INDEX_CREATED) {
                            // TODO: we could include more cases that would cause early publishing of desired balance in case of a long
                            // computation. e.g.:
                            // - unassigned search replicas in case the shard has no assigned shard replicas
                            // - other reasons for an unassigned shard such as NEW_INDEX_RESTORED
                            assignedNewlyCreatedPrimaryShards = true;
                        }
                        clusterInfoSimulator.simulateShardStarted(shardRouting);
                        routingNodes.startShard(shardRouting, changes, 0L);
                    }
                }
            }

            i += 1;
            numIterationsSinceLastConverged += 1;
            final int iterations = i;
            final long currentTime = timeProvider.relativeTimeInMillis();
            final boolean reportByTime = nextReportTime <= currentTime;
            final boolean reportByIterationCount = numIterationsSinceLastConverged % iterationCountReportInterval == 0;
            if (reportByTime || reportByIterationCount) {
                nextReportTime = currentTime + timeWarningInterval;
            }

            if (hasChanges == false && hasEnoughIterations(i)) {
                if (numComputeCallsSinceLastConverged > 1) {
                    logger.log(
                        convergenceLogMsgLevel,
                        () -> Strings.format(
                            """
                                Desired balance computation for [%d] converged after [%s] and [%d] iterations, \
                                resumed computation [%d] times with [%d] iterations since the last resumption [%s] ago""",
                            desiredBalanceInput.index(),
                            TimeValue.timeValueMillis(currentTime - lastConvergedTimeMillis).toString(),
                            numIterationsSinceLastConverged,
                            numComputeCallsSinceLastConverged,
                            iterations,
                            TimeValue.timeValueMillis(currentTime - computationStartedTime).toString()
                        )
                    );
                } else {
                    logger.log(
                        convergenceLogMsgLevel,
                        () -> Strings.format(
                            "Desired balance computation for [%d] converged after [%s] and [%d] iterations",
                            desiredBalanceInput.index(),
                            TimeValue.timeValueMillis(currentTime - lastConvergedTimeMillis).toString(),
                            numIterationsSinceLastConverged
                        )
                    );
                }
                numComputeCallsSinceLastConverged = 0;
                numIterationsSinceLastConverged = 0;
                lastConvergedTimeMillis = currentTime;
                break;
            }
            if (isFresh.test(desiredBalanceInput) == false) {
                // we run at least one iteration, but if another reroute happened meanwhile
                // then publish the interim state and restart the calculation
                logger.debug(
                    "Desired balance computation for [{}] interrupted after [{}] and [{}] iterations as newer cluster state received. "
                        + "Publishing intermediate desired balance and restarting computation",
                    desiredBalanceInput.index(),
                    TimeValue.timeValueMillis(currentTime - computationStartedTime).toString(),
                    i
                );
                finishReason = DesiredBalance.ComputationFinishReason.YIELD_TO_NEW_INPUT;
                break;
            }

            if (assignedNewlyCreatedPrimaryShards
                && currentTime - computationStartedTime >= maxBalanceComputationTimeDuringIndexCreationMillis) {
                logger.info(
                    "Desired balance computation for [{}] interrupted after [{}] and [{}] iterations "
                        + "in order to not delay assignment of newly created index shards for more than [{}]. "
                        + "Publishing intermediate desired balance and restarting computation",
                    desiredBalanceInput.index(),
                    TimeValue.timeValueMillis(currentTime - computationStartedTime).toString(),
                    i,
                    TimeValue.timeValueMillis(maxBalanceComputationTimeDuringIndexCreationMillis).toString()
                );
                finishReason = DesiredBalance.ComputationFinishReason.STOP_EARLY;
                break;
            }

            final var logLevel = reportByIterationCount || reportByTime ? Level.INFO : i % 100 == 0 ? Level.DEBUG : Level.TRACE;
            if (numComputeCallsSinceLastConverged > 1) {
                logger.log(
                    logLevel,
                    () -> Strings.format(
                        """
                            Desired balance computation for [%d] is still not converged after [%s] and [%d] iterations, \
                            resumed computation [%d] times with [%d] iterations since the last resumption [%s] ago""",
                        desiredBalanceInput.index(),
                        TimeValue.timeValueMillis(currentTime - lastConvergedTimeMillis).toString(),
                        numIterationsSinceLastConverged,
                        numComputeCallsSinceLastConverged,
                        iterations,
                        TimeValue.timeValueMillis(currentTime - computationStartedTime).toString()
                    )
                );
            } else {
                logger.log(
                    logLevel,
                    () -> Strings.format(
                        "Desired balance computation for [%d] is still not converged after [%s] and [%d] iterations",
                        desiredBalanceInput.index(),
                        TimeValue.timeValueMillis(currentTime - lastConvergedTimeMillis).toString(),
                        numIterationsSinceLastConverged
                    )
                );
            }

            if (reportByIterationCount || reportByTime) {
                lastNotConvergedLogMessageTimeMillis = currentTime;
            }
        }
        iterations.inc(i);

        final var assignments = collectShardAssignments(routingNodes);

        for (var shard : routingNodes.unassigned().ignored()) {
            var info = shard.unassignedInfo();
            assert info != null
                && (info.lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO
                    || info.lastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT
                    || info.lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED) : "Unexpected stats in: " + info;

            if (hasChanges == false && info.lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED) {
                // Simulation could not progress due to missing information in any of the deciders.
                // Currently, this could happen if `HasFrozenCacheAllocationDecider` is still fetching the data.
                // Progress would be made after the followup reroute call.
                hasChanges = true;
            }

            var ignored = shard.unassignedInfo().lastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO ? 0 : 1;
            assignments.compute(
                shard.shardId(),
                (key, oldValue) -> oldValue == null
                    ? new ShardAssignment(Set.of(), 1, 1, ignored)
                    : new ShardAssignment(oldValue.nodeIds(), oldValue.total() + 1, oldValue.unassigned() + 1, oldValue.ignored() + ignored)
            );
        }

        long lastConvergedIndex = hasChanges ? previousDesiredBalance.lastConvergedIndex() : desiredBalanceInput.index();
        return new DesiredBalance(lastConvergedIndex, assignments, routingNodes.getBalanceWeightStatsPerNode(), finishReason);
    }

    // visible for testing
    boolean hasEnoughIterations(int currentIteration) {
        return true;
    }

    private static Map<ShardId, ShardAssignment> collectShardAssignments(RoutingNodes routingNodes) {
        final var allAssignedShards = routingNodes.getAssignedShards().entrySet();
        assert allAssignedShards.stream().flatMap(t -> t.getValue().stream()).allMatch(ShardRouting::started) : routingNodes;
        final Map<ShardId, ShardAssignment> res = Maps.newHashMapWithExpectedSize(allAssignedShards.size());
        for (var shardIdAndShardRoutings : allAssignedShards) {
            res.put(
                shardIdAndShardRoutings.getKey(),
                ShardAssignment.createFromAssignedShardRoutingsList(shardIdAndShardRoutings.getValue())
            );
        }
        return res;
    }

    private record ShardRoutings(List<ShardRouting> unassigned, List<ShardRouting> assigned) {

        private ShardRoutings(ShardId ignored) {
            this(new ArrayList<>(), new ArrayList<>());
        }
    }

    private static Set<ShardRouting> getIgnoredShardsWithDiscardedAllocationStatus(List<ShardRouting> ignoredShards) {
        return ignoredShards.stream().map(DesiredBalanceComputer::discardAllocationStatus).collect(toUnmodifiableSet());
    }

    /**
     * AllocationStatus is discarded as it might come from GatewayAllocator and not be present in corresponding routing table
     */
    private static ShardRouting discardAllocationStatus(ShardRouting shardRouting) {
        return shardRouting.updateUnassigned(discardAllocationStatus(shardRouting.unassignedInfo()), shardRouting.recoverySource());
    }

    private static UnassignedInfo discardAllocationStatus(UnassignedInfo info) {
        if (info.lastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT) {
            return info;
        }
        return new UnassignedInfo(
            info.reason(),
            info.message(),
            info.failure(),
            info.failedAllocations(),
            info.unassignedTimeNanos(),
            info.unassignedTimeMillis(),
            info.delayed(),
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            info.failedNodeIds(),
            info.lastAllocatedNodeId()
        );
    }

    private static int computeIterationCountReportInterval(RoutingAllocation allocation) {
        final int relativeSize = allocation.metadata().getTotalNumberOfShards();
        int iterations = 1000;
        while (iterations < relativeSize && iterations < 1_000_000_000) {
            iterations *= 10;
        }
        return iterations;
    }

    // Package-level getters for testing.
    long getNumComputeCallsSinceLastConverged() {
        return numComputeCallsSinceLastConverged;
    }

    long getNumIterationsSinceLastConverged() {
        return numIterationsSinceLastConverged;
    }

    long getLastConvergedTimeMillis() {
        return lastConvergedTimeMillis;
    }

    void setConvergenceLogMsgLevel(Level level) {
        convergenceLogMsgLevel = level;
    }
}
