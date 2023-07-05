/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

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

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Holds the desired balance and updates it as the cluster evolves.
 */
public class DesiredBalanceComputer {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceComputer.class);

    private final ThreadPool threadPool;
    private final ShardsAllocator delegateAllocator;

    protected final MeanMetric iterations = new MeanMetric();

    public static final Setting<TimeValue> PROGRESS_LOG_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.desired_balance.progress_log_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue progressLogInterval;

    public DesiredBalanceComputer(ClusterSettings clusterSettings, ThreadPool threadPool, ShardsAllocator delegateAllocator) {
        this.threadPool = threadPool;
        this.delegateAllocator = delegateAllocator;
        clusterSettings.initializeAndWatch(PROGRESS_LOG_INTERVAL_SETTING, value -> this.progressLogInterval = value);
    }

    public DesiredBalance compute(
        DesiredBalance previousDesiredBalance,
        DesiredBalanceInput desiredBalanceInput,
        Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
        Predicate<DesiredBalanceInput> isFresh
    ) {

        logger.debug("Recomputing desired balance for [{}]", desiredBalanceInput.index());

        final var routingAllocation = desiredBalanceInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = routingAllocation.routingNodes();
        final var changes = routingAllocation.changes();
        final var ignoredShards = getIgnoredShardsWithDiscardedAllocationStatus(desiredBalanceInput.ignoredShards());
        final var knownNodeIds = routingAllocation.nodes().stream().map(DiscoveryNode::getId).collect(toSet());
        final var clusterInfoSimulator = new ClusterInfoSimulator(routingAllocation.clusterInfo());

        if (routingNodes.size() == 0) {
            return new DesiredBalance(desiredBalanceInput.index(), Map.of());
        }

        // we assume that all ongoing recoveries will complete
        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                if (shardRouting.initializing()) {
                    clusterInfoSimulator.simulateShardStarted(shardRouting);
                    routingNodes.startShard(logger, shardRouting, changes, 0L);
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
                    var lastAllocatedNodeId = shardRouting.unassignedInfo().getLastAllocatedNodeId();
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

        // we can assume that all possible shards will be allocated/relocated to one of their desired locations
        final var unassignedShardsToInitialize = new HashMap<ShardRouting, LinkedList<String>>();
        for (final var entry : shardRoutings.entrySet()) {
            final var shardId = entry.getKey();
            final var routings = entry.getValue();

            // treemap (keyed by node ID) so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeMap<String, ShardRouting>();
            final var assignment = previousDesiredBalance.getAssignment(shardId);

            // treeset (ordered by node ID) so that we are consistent about the order of future relocations
            final var targetNodes = assignment != null ? new TreeSet<>(assignment.nodeIds()) : new TreeSet<String>();
            targetNodes.retainAll(knownNodeIds);

            // preserving last known shard location as a starting point to avoid unnecessary relocations
            for (ShardRouting shardRouting : routings.unassigned()) {
                var lastAllocatedNodeId = shardRouting.unassignedInfo().getLastAllocatedNodeId();
                if (knownNodeIds.contains(lastAllocatedNodeId)) {
                    targetNodes.add(lastAllocatedNodeId);
                }
            }

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
                        && routingAllocation.deciders()
                            .canAllocate(shardRouting, targetNode, routingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToRelocate = routingNodes.relocateShard(shardRouting, targetNodeId, 0L, changes).v2();
                        clusterInfoSimulator.simulateShardStarted(shardToRelocate);
                        routingNodes.startShard(logger, shardToRelocate, changes, 0L);
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
                        && routingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, routingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToInitialize = unassignedPrimaryIterator.initialize(nodeId, null, 0L, changes);
                        clusterInfoSimulator.simulateShardStarted(shardToInitialize);
                        routingNodes.startShard(logger, shardToInitialize, changes, 0L);
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
                        && routingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, routingAllocation)
                            .type() != Decision.Type.NO) {
                        final var shardToInitialize = unassignedReplicaIterator.initialize(nodeId, null, 0L, changes);
                        clusterInfoSimulator.simulateShardStarted(shardToInitialize);
                        routingNodes.startShard(logger, shardToInitialize, changes, 0L);
                    }
                }
            }
        }

        List<MoveAllocationCommand> commands;
        while ((commands = pendingDesiredBalanceMoves.poll()) != null) {
            for (MoveAllocationCommand command : commands) {
                try {
                    command.execute(routingAllocation, false);
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

        final int iterationCountReportInterval = computeIterationCountReportInterval(routingAllocation);
        final long timeWarningInterval = progressLogInterval.millis();
        final long computationStartedTime = threadPool.relativeTimeInMillis();
        long nextReportTime = computationStartedTime + timeWarningInterval;

        int i = 0;
        boolean hasChanges = false;
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

            routingAllocation.setSimulatedClusterInfo(clusterInfoSimulator.getClusterInfo());
            logger.trace("running delegate allocator");
            delegateAllocator.allocate(routingAllocation);
            assert routingNodes.unassigned().size() == 0; // any unassigned shards should now be ignored

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        clusterInfoSimulator.simulateShardStarted(shardRouting);
                        routingNodes.startShard(logger, shardRouting, changes, 0L);
                        logger.trace("starting shard {}", shardRouting);
                    }
                }
            }

            i++;
            final int iterations = i;
            final long currentTime = threadPool.relativeTimeInMillis();
            final boolean reportByTime = nextReportTime <= currentTime;
            final boolean reportByIterationCount = i % iterationCountReportInterval == 0;
            if (reportByTime || reportByIterationCount) {
                nextReportTime = currentTime + timeWarningInterval;
            }

            if (hasChanges == false) {
                logger.debug(
                    "Desired balance computation for [{}] converged after [{}] and [{}] iterations",
                    desiredBalanceInput.index(),
                    TimeValue.timeValueMillis(currentTime - computationStartedTime).toString(),
                    i
                );
                break;
            }
            if (isFresh.test(desiredBalanceInput) == false) {
                // we run at least one iteration, but if another reroute happened meanwhile
                // then publish the interim state and restart the calculation
                logger.debug(
                    "Desired balance computation for [{}] interrupted after [{}] and [{}] iterations as newer cluster state received. "
                        + "Publishing intermediate desired balance and restarting computation",
                    desiredBalanceInput.index(),
                    i,
                    TimeValue.timeValueMillis(currentTime - computationStartedTime).toString()
                );
                break;
            }

            logger.log(
                reportByIterationCount || reportByTime ? Level.INFO : i % 100 == 0 ? Level.DEBUG : Level.TRACE,
                () -> Strings.format(
                    "Desired balance computation for [%d] is still not converged after [%s] and [%d] iterations",
                    desiredBalanceInput.index(),
                    TimeValue.timeValueMillis(currentTime - computationStartedTime).toString(),
                    iterations
                )
            );
        }
        iterations.inc(i);

        final var assignments = new HashMap<ShardId, ShardAssignment>();
        for (var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            assignments.put(shardAndAssignments.getKey(), ShardAssignment.ofAssignedShards(shardAndAssignments.getValue()));
        }

        for (var shard : routingNodes.unassigned().ignored()) {
            var info = shard.unassignedInfo();
            assert info != null
                && (info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO
                    || info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT
                    || info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED)
                : "Unexpected stats in: " + info;

            if (hasChanges == false && info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_THROTTLED) {
                // Simulation could not progress due to missing information in any of the deciders.
                // Currently, this could happen if `HasFrozenCacheAllocationDecider` is still fetching the data.
                // Progress would be made after the followup reroute call.
                hasChanges = true;
            }

            var ignored = shard.unassignedInfo().getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO ? 0 : 1;
            assignments.compute(
                shard.shardId(),
                (key, oldValue) -> oldValue == null
                    ? new ShardAssignment(Set.of(), 1, 1, ignored)
                    : new ShardAssignment(oldValue.nodeIds(), oldValue.total() + 1, oldValue.unassigned() + 1, oldValue.ignored() + ignored)
            );
        }

        long lastConvergedIndex = hasChanges ? previousDesiredBalance.lastConvergedIndex() : desiredBalanceInput.index();
        return new DesiredBalance(lastConvergedIndex, assignments);
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
        if (info.getLastAllocationStatus() == UnassignedInfo.AllocationStatus.NO_ATTEMPT) {
            return info;
        }
        return new UnassignedInfo(
            info.getReason(),
            info.getMessage(),
            info.getFailure(),
            info.getNumFailedAllocations(),
            info.getUnassignedTimeInNanos(),
            info.getUnassignedTimeInMillis(),
            info.isDelayed(),
            UnassignedInfo.AllocationStatus.NO_ATTEMPT,
            info.getFailedNodeIds(),
            info.getLastAllocatedNodeId()
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
}
