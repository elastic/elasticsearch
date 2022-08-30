/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Holds the desired balance and updates it as the cluster evolves.
 */
public class DesiredBalanceComputer {

    private static final Logger logger = LogManager.getLogger(DesiredBalanceComputer.class);

    private final ShardsAllocator delegateAllocator;

    public DesiredBalanceComputer(ShardsAllocator delegateAllocator) {
        this.delegateAllocator = delegateAllocator;
    }

    public DesiredBalance compute(
        DesiredBalance previousDesiredBalance,
        DesiredBalanceInput desiredBalanceInput,
        Queue<List<MoveAllocationCommand>> pendingDesiredBalanceMoves,
        Predicate<DesiredBalanceInput> isFresh
    ) {

        logger.trace("starting to recompute desired balance for [{}]", desiredBalanceInput.index());

        final var routingAllocation = desiredBalanceInput.routingAllocation().mutableCloneForSimulation();
        final var routingNodes = routingAllocation.routingNodes();
        final var ignoredShards = desiredBalanceInput.ignoredShards();
        final var changes = routingAllocation.changes();
        final var knownNodeIds = routingAllocation.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        final var unassignedPrimaries = new HashSet<ShardId>();

        if (routingNodes.isEmpty()) {
            return new DesiredBalance(desiredBalanceInput.index(), Map.of());
        }

        // we assume that all ongoing recoveries will complete
        for (final var routingNode : routingNodes) {
            for (final var shardRouting : routingNode) {
                if (shardRouting.initializing()) {
                    routingNodes.startShard(logger, shardRouting, changes);
                    // TODO adjust disk usage info to reflect the assumed shard movement
                }
            }
        }

        // we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
        // unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
        final var shardRoutings = new HashMap<ShardId, ShardRoutings>();
        for (final var primary : new boolean[] { true, false }) {
            final RoutingNodes.UnassignedShards unassigned = routingNodes.unassigned();
            for (final var iterator = unassigned.iterator(); iterator.hasNext();) {
                final var shardRouting = iterator.next();
                if (shardRouting.primary() == primary) {
                    if (ignoredShards.contains(shardRouting)) {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                        if (shardRouting.primary()) {
                            unassignedPrimaries.add(shardRouting.shardId());
                        }
                    } else {
                        shardRoutings.computeIfAbsent(shardRouting.shardId(), ShardRoutings::new).unassigned().add(shardRouting);
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

            // treesets so that we are consistent about the order of future relocations
            final var shardsToRelocate = new TreeSet<>(Comparator.comparing(ShardRouting::currentNodeId));
            final var assignment = previousDesiredBalance.getAssignment(shardId);

            final var targetNodes = assignment != null ? new TreeSet<>(assignment.nodeIds()) : new TreeSet<String>();
            targetNodes.retainAll(knownNodeIds);

            for (final var shardRouting : routings.assigned()) {
                assert shardRouting.started();
                if (targetNodes.remove(shardRouting.currentNodeId()) == false) {
                    shardsToRelocate.add(shardRouting);
                }
            }

            final var targetNodesIterator = targetNodes.iterator();

            // Here existing shards are moved to desired locations before initializing unassigned shards because we prefer not to leave
            // immovable shards allocated to undesirable locations (e.g. a node that is shutting down). In contrast, reconciliation prefers
            // to initialize the unassigned shards first.
            for (final var shardRouting : shardsToRelocate) {
                assert shardRouting.started();
                if (targetNodesIterator.hasNext()) {
                    routingNodes.startShard(
                        logger,
                        routingNodes.relocateShard(shardRouting, targetNodesIterator.next(), 0L, changes).v2(),
                        changes
                    );
                } else {
                    break;
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
                    final String nodeId = nodeIds.removeFirst();
                    routingNodes.startShard(logger, unassignedPrimaryIterator.initialize(nodeId, null, 0L, changes), changes);
                }
            }
        }

        final var unassignedReplicaIterator = routingNodes.unassigned().iterator();
        while (unassignedReplicaIterator.hasNext()) {
            final var shardRouting = unassignedReplicaIterator.next();
            if (unassignedPrimaries.contains(shardRouting.shardId()) == false) {
                final var nodeIds = unassignedShardsToInitialize.get(shardRouting);
                if (nodeIds != null && nodeIds.isEmpty() == false) {
                    final String nodeId = nodeIds.removeFirst();
                    routingNodes.startShard(logger, unassignedReplicaIterator.initialize(nodeId, null, 0L, changes), changes);
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

        // TODO must also bypass ResizeAllocationDecider
        // TODO must also bypass RestoreInProgressAllocationDecider
        // TODO what about delayed allocation?

        // TODO consider also whether to unassign any shards that cannot remain on their current nodes so that the desired balance reflects
        // the actual desired state of the cluster. But this would mean that a REPLACE shutdown needs special handling at reconciliation
        // time too. But maybe it needs special handling anyway since reconciliation also tries to respect allocation rules.

        boolean hasChanges = false;
        for (int i = 0; true; i++) {
            if (hasChanges) {
                final var unassigned = routingNodes.unassigned();

                // Not the first iteration, so every remaining unassigned shard has been ignored, perhaps due to throttling. We must bring
                // them all back out of the ignored list to give the allocator another go...
                unassigned.resetIgnored();

                // ... but not if they're ignored because they're out of scope for allocation
                for (final var iterator = unassigned.iterator(); iterator.hasNext();) {
                    final var shardRouting = iterator.next();
                    if (ignoredShards.contains(shardRouting)) {
                        iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, changes);
                    }
                }

                // TODO test that we reset ignored shards properly
            }

            logger.trace("running delegate allocator");
            delegateAllocator.allocate(routingAllocation);
            assert routingNodes.unassigned().size() == 0; // any unassigned shards should now be ignored

            hasChanges = false;
            for (final var routingNode : routingNodes) {
                for (final var shardRouting : routingNode) {
                    if (shardRouting.initializing()) {
                        hasChanges = true;
                        routingNodes.startShard(logger, shardRouting, changes);
                        logger.trace("starting shard {}", shardRouting);
                        // TODO adjust disk usage info to reflect the assumed shard movement
                    }
                }
            }

            // TODO what if we never converge?
            // TODO maybe expose interim desired balances computed here

            if (hasChanges == false) {
                logger.trace("desired balance computation converged after {} iterations", i);
                break;
            }
            if (isFresh.test(desiredBalanceInput) == false) {
                // we run at least one iteration, but if another reroute happened meanwhile
                // then publish the interim state and restart the calculation
                logger.trace("newer cluster state received, publishing incomplete desired balance and restarting computation");
                break;
            }
            if (i > 0 && i % 100 == 0) {
                logger.warn("desired balance computation is still not converged after {} iterations", i);
            }
        }

        final var assignments = new HashMap<ShardId, ShardAssignment>();
        for (var shardAndAssignments : routingNodes.getAssignedShards().entrySet()) {
            assignments.put(shardAndAssignments.getKey(), ShardAssignment.of(shardAndAssignments.getValue()));
        }

        for (var ignored : routingNodes.unassigned().ignored()) {
            var info = ignored.unassignedInfo();
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

            var unassigned = ignored.unassignedInfo().getLastAllocationStatus() == UnassignedInfo.AllocationStatus.DECIDERS_NO;
            assignments.compute(
                ignored.shardId(),
                (key, oldValue) -> oldValue == null
                    ? new ShardAssignment(Set.of(), 1, 1, unassigned ? 0 : 1)
                    : new ShardAssignment(
                        oldValue.nodeIds(),
                        oldValue.total() + 1,
                        oldValue.unassigned() + 1,
                        oldValue.ignored() + (unassigned ? 0 : 1)
                    )
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
}
