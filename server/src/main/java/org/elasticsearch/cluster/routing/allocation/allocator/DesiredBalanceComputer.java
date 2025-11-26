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
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoSimulator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator.ShardAllocationExplainer;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Holds the desired balance and updates it as the cluster evolves.
 */
public class DesiredBalanceComputer {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceComputer.class);
    private static final Logger allocationExplainLogger = LogManager.getLogger(
        DesiredBalanceComputer.class.getCanonicalName() + ".allocation_explain"
    );

    private final ShardsAllocator delegateAllocator;
    private final TimeProvider timeProvider;
    private final ShardAllocationExplainer shardAllocationExplainer;

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
    private ShardRouting lastTrackedUnassignedShard;

    public DesiredBalanceComputer(
        ClusterSettings clusterSettings,
        TimeProvider timeProvider,
        ShardsAllocator delegateAllocator,
        ShardAllocationExplainer shardAllocationExplainer
    ) {
        this.delegateAllocator = delegateAllocator;
        this.timeProvider = timeProvider;
        this.shardAllocationExplainer = shardAllocationExplainer;
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

        final var routingAllocation = desiredBalanceInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = routingAllocation.routingNodes();
        final var knownNodeIds = routingNodes.getAllNodeIds();
        final var changes = routingAllocation.changes();
        final var ignoredShards = getIgnoredShardsWithDiscardedAllocationStatus(desiredBalanceInput.ignoredShards());
        final var clusterInfoSimulator = new ClusterInfoSimulator(routingAllocation);
        DesiredBalance.ComputationFinishReason finishReason = DesiredBalance.ComputationFinishReason.CONVERGED;

        if (routingNodes.size() == 0) {
            return new DesiredBalance(desiredBalanceInput.index(), Map.of(), Map.of(), finishReason);
        }

        maybeSimulateAlreadyStartedShards(desiredBalanceInput.routingAllocation().clusterInfo(), routingNodes, clusterInfoSimulator);

        // we assume that all ongoing recoveries will complete
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
                var lastAllocatedNodeId = shardRouting.unassignedInfo().lastAllocatedNodeId();
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
                        && routingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, routingAllocation)
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
                        && routingAllocation.deciders()
                            .canAllocate(shardRouting, routingNode, routingAllocation)
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
                    final var rerouteExplanation = command.execute(routingAllocation, false);
                    assert rerouteExplanation.decisions().type() != Decision.Type.NO : "should have thrown for NO decision";
                    if (rerouteExplanation.decisions().type() != Decision.Type.NO) {
                        final Iterator<ShardRouting> initializingShardsIterator = routingNodes.node(
                            routingAllocation.nodes().resolveNode(command.toNode()).getId()
                        ).initializing().iterator();
                        assert initializingShardsIterator.hasNext();
                        final var initializingShard = initializingShardsIterator.next();
                        assert initializingShardsIterator.hasNext() == false
                            : "expect exactly one relocating shard, but got: "
                                + Iterators.toList(Iterators.concat(Iterators.single(initializingShard), initializingShardsIterator));
                        assert routingAllocation.nodes()
                            .resolveNode(command.fromNode())
                            .getId()
                            .equals(initializingShard.relocatingNodeId())
                            : initializingShard
                                + " has unexpected relocation source node, expect node "
                                + routingAllocation.nodes().resolveNode(command.fromNode());
                        clusterInfoSimulator.simulateShardStarted(initializingShard);
                        routingNodes.startShard(initializingShard, changes, 0L);
                    }
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

            routingAllocation.setSimulatedClusterInfo(clusterInfoSimulator.getClusterInfo());
            logger.trace("running delegate allocator");
            delegateAllocator.allocate(routingAllocation);
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
                // Unassigned ignored shards must be based on the provided set of ignoredShards
                assert ignoredShards.contains(discardAllocationStatus(shard))
                    || ignoredShards.stream().filter(ShardRouting::primary).anyMatch(primary -> primary.shardId().equals(shard.shardId()))
                    : "ignored shard "
                        + shard
                        + " unexpectedly has THROTTLE status and no counterpart in the provided ignoredShards set "
                        + ignoredShards;
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

        maybeLogAllocationExplainForUnassigned(finishReason, routingNodes, routingAllocation, desiredBalanceInput.index());

        long lastConvergedIndex = hasChanges ? previousDesiredBalance.lastConvergedIndex() : desiredBalanceInput.index();
        return new DesiredBalance(lastConvergedIndex, assignments, routingNodes.getBalanceWeightStatsPerNode(), finishReason);
    }

    /**
     * For shards started after initial polling of the ClusterInfo but before the next polling, we need to
     * account for their impacts by simulating the events, either relocation or new shard start. This is done
     * by comparing the current RoutingNodes against the shard allocation information from the ClusterInfo to
     * find out the shard allocation changes. Note this approach is approximate in some edge cases:
     * <ol>
     * <li> If a shard is relocated twice from node A to B to C. It is considered as relocating from A to C directly
     * for simulation purpose.</li>
     * <li>If a shard has 2 replicas and they both relocate, replica 1 from A to X and replica 2 from B to Y. The
     * simulation may see them as relocations A->X and B->Y. But it may also see them as A->Y and B->X. </li>
     * </ol>
     * In both cases, it should not really matter for simulation to account for resource changes.
     */
    static void maybeSimulateAlreadyStartedShards(
        ClusterInfo clusterInfo,
        RoutingNodes routingNodes,
        ClusterInfoSimulator clusterInfoSimulator
    ) {
        // Find all shards that are started in RoutingNodes but have no data on corresponding node in ClusterInfo
        final var startedShards = new ArrayList<ShardRouting>();
        for (var routingNode : routingNodes) {
            for (var shardRouting : routingNode.started()) {
                if (clusterInfo.hasShardMoved(shardRouting)) {
                    startedShards.add(shardRouting);
                }
            }
        }
        if (startedShards.isEmpty()) {
            return;
        }
        logger.debug(
            "Found [{}] started shards not accounted in ClusterInfo. The first one is {}",
            startedShards.size(),
            startedShards.getFirst()
        );

        // For started shards, attempt to find its source node. If found, it is a relocation, otherwise it is a new shard.
        // The same shard on the same source node cannot be relocated twice to different nodes. So we exclude it once used.
        final Map<ShardId, Set<String>> alreadySeenSourceNodes = new HashMap<>();
        for (var startedShard : startedShards) {
            // The source node is found by checking whether the ClusterInfo has a node hosting a shard with the same ShardId
            // and has compatible node role. If multiple nodes are found, simply pick the first one.
            final var sourceNodeId = clusterInfo.getNodeIdsForShard(startedShard.shardId())
                .stream()
                // Do not use the same source node twice for the same shard
                .filter(nodeId -> alreadySeenSourceNodes.getOrDefault(startedShard.shardId(), Set.of()).contains(nodeId) == false)
                .map(routingNodes::node)
                // The source node must not currently host the shard
                .filter(routingNode -> routingNode != null && routingNode.getByShardId(startedShard.shardId()) == null)
                .map(RoutingNode::node)
                // The source node must have compatible node roles
                .filter(node -> node != null && switch (startedShard.role()) {
                    case DEFAULT -> node.canContainData();
                    case INDEX_ONLY -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE);
                    case SEARCH_ONLY -> node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE);
                })
                .map(DiscoveryNode::getId)
                .findFirst()
                .orElse(null);

            if (sourceNodeId != null) {
                alreadySeenSourceNodes.computeIfAbsent(startedShard.shardId(), k -> new HashSet<>()).add(sourceNodeId);
            }
            clusterInfoSimulator.simulateAlreadyStartedShard(startedShard, sourceNodeId);
        }
    }

    private void maybeLogAllocationExplainForUnassigned(
        DesiredBalance.ComputationFinishReason finishReason,
        RoutingNodes routingNodes,
        RoutingAllocation routingAllocation,
        long inputIndex
    ) {
        if (allocationExplainLogger.isDebugEnabled()) {
            final var clusterState = routingAllocation.getClusterState();
            if (clusterState.metadata().nodeShutdowns().contains(clusterState.nodes().getLocalNodeId())) {
                // Master is shutting down, the project is likely being deleted. Shards can be unassigned and no need to report
                return;
            }
            if (lastTrackedUnassignedShard != null) {
                if (Stream.concat(routingNodes.unassigned().stream(), routingNodes.unassigned().ignored().stream())
                    .noneMatch(shardRouting -> shardRouting.equals(lastTrackedUnassignedShard))) {
                    allocationExplainLogger.debug(
                        "computation for input index [{}] assigned previously tracked unassigned shard [{}]",
                        inputIndex,
                        lastTrackedUnassignedShard
                    );
                    lastTrackedUnassignedShard = null;
                } else {
                    return; // The last tracked unassigned shard is still unassigned, keep tracking it
                }
            }

            assert lastTrackedUnassignedShard == null : "unexpected non-null lastTrackedUnassignedShard " + lastTrackedUnassignedShard;
            if (routingNodes.hasUnassignedShards() && finishReason == DesiredBalance.ComputationFinishReason.CONVERGED) {
                final Predicate<ShardRouting> predicate = routingNodes.hasUnassignedPrimaries() ? ShardRouting::primary : shard -> true;
                lastTrackedUnassignedShard = Stream.concat(routingNodes.unassigned().stream(), routingNodes.unassigned().ignored().stream())
                    .filter(predicate)
                    .findFirst()
                    .orElseThrow();

                final var originalDebugMode = routingAllocation.getDebugMode();
                routingAllocation.setDebugMode(RoutingAllocation.DebugMode.EXCLUDE_YES_DECISIONS);
                final ShardAllocationDecision shardAllocationDecision;
                try {
                    shardAllocationDecision = shardAllocationExplainer.explain(lastTrackedUnassignedShard, routingAllocation);
                } finally {
                    routingAllocation.setDebugMode(originalDebugMode);
                }
                allocationExplainLogger.debug(
                    "computation converged for input index [{}] with unassigned shard [{}] due to allocation decision {}",
                    inputIndex,
                    lastTrackedUnassignedShard,
                    org.elasticsearch.common.Strings.toString(
                        p -> ChunkedToXContentHelper.object("node_allocation_decision", shardAllocationDecision.toXContentChunked(p))
                    )
                );
            }
        } else {
            if (lastTrackedUnassignedShard != null) {
                lastTrackedUnassignedShard = null;
            }
        }
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
