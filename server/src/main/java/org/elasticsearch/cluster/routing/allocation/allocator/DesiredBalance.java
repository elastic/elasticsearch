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
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The desired balance of the cluster, indicating which nodes should hold a copy of each shard.
 *
 * @param lastConvergedIndex Identifies what input data the balancer computation round used to produce this {@link DesiredBalance}. See
 *                           {@link DesiredBalanceInput#index()} for details. Each reroute request in the same master term is assigned a
 *                           strictly increasing sequence number. A new master term restarts the index values from zero. The balancer,
 *                           which runs async to reroute, uses the latest request's data to compute the desired balance.
 * @param assignments a set of the (persistent) node IDs to which each {@link ShardId} should be allocated
 * @param weightsPerNode The node weights calculated based on {@link WeightFunction#calculateNodeWeight}
 */
public record DesiredBalance(
    long lastConvergedIndex,
    Map<ShardId, ShardAssignment> assignments,
    Map<DiscoveryNode, DesiredBalanceMetrics.NodeWeightStats> weightsPerNode,
    ComputationFinishReason finishReason
) {

    enum ComputationFinishReason {
        /**
         * Computation ran to completion
         */
        CONVERGED,
        /**
         * Computation exited and published early because a new cluster event occurred that affects computation
         */
        YIELD_TO_NEW_INPUT,
        /**
         * Computation stopped and published early to avoid delaying new shard assignment
         */
        STOP_EARLY
    }

    public DesiredBalance(long lastConvergedIndex, Map<ShardId, ShardAssignment> assignments) {
        this(lastConvergedIndex, assignments, Map.of(), ComputationFinishReason.CONVERGED);
    }

    /**
     * The placeholder value for {@link DesiredBalance} when the node stands down as master.
     */
    public static final DesiredBalance NOT_MASTER = new DesiredBalance(-2, Map.of());

    /**
     * The starting value for {@link DesiredBalance} when the node becomes the master.
     */
    public static final DesiredBalance BECOME_MASTER_INITIAL = new DesiredBalance(-1, Map.of());

    public ShardAssignment getAssignment(ShardId shardId) {
        return assignments.get(shardId);
    }

    public static boolean hasChanges(DesiredBalance a, DesiredBalance b) {
        return Objects.equals(a.assignments, b.assignments) == false;
    }

    /**
     * Returns the sum of shard movements needed to reach the new desired balance. Doesn't count new shard copies as a move, nor removal or
     * unassignment of a shard copy.
     */
    public static int shardMovements(DesiredBalance old, DesiredBalance updated) {
        // The intersection of assigned shards in both the old and updated DesiredBalance removes entirely new or deleted shards.
        var intersection = Sets.intersection(old.assignments().keySet(), updated.assignments().keySet());
        int movements = 0;
        for (ShardId shardId : intersection) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            if (Objects.equals(oldAssignment, updatedAssignment) == false) {
                // Now we need to check whether any of the shard copy assignments changed nodes.
                movements += shardMovements(oldAssignment, updatedAssignment);
            }
        }
        return movements;
    }

    public record ShardMoveCounts(int movements, int newlyAssigned, int newlyUnassigned, int shutdownInducedMoves) {}

    public static ShardMoveCounts shardAssignmentChanges(DesiredBalance old, DesiredBalance updated) {
        var diff = Sets.difference(old.weightsPerNode.keySet(), updated.weightsPerNode.keySet());
        Set<String> nodeIdsRemoved = diff.stream().map(node -> node.getId()).collect(Collectors.toSet());
        int numCopiesMoved = 0;
        int numCopiesNewlyAssigned = 0;
        int numCopiesNewlyUnassigned = 0;
        int numCopiesMovedDueToShutdown = 0;

        // The intersection of assigned shards in both the old and updated DesiredBalance removes entirely new or deleted shards.
        var continuingShardIds = Sets.intersection(old.assignments().keySet(), updated.assignments().keySet());

        // The shard IDs that were not assigned in the 'old' desired balance.
        var newlyAssignedShardIds = Sets.difference(updated.assignments.keySet(), old.assignments.keySet());

        // The shard IDs that are not assigned in the 'updated' desired balance.
        var newlyUnassignedShardIds = Sets.difference(old.assignments.keySet(), updated.assignments.keySet());

        // Count all the copies of newly assigned index shards: shard IDs that were not assigned in the 'old' desired balance.
        for (var newlyAssignedIndexShardId : newlyAssignedShardIds) {
            // Note that this might be zero if none of the new shards have been assigned yet.
            numCopiesNewlyAssigned += updated.getAssignment(newlyAssignedIndexShardId).assigned();
        }

        // Count all the copies that were assigned in the old desired balance, and no longer exist in the updated desired balance.
        for (var newlyUnassignedIndexShardId : newlyUnassignedShardIds) {
            numCopiesNewlyUnassigned += old.getAssignment(newlyUnassignedIndexShardId).assigned();
        }

        for (ShardId shardId : continuingShardIds) {
            var oldShardAssignment = old.getAssignment(shardId);
            var updatedShardAssignment = updated.getAssignment(shardId);

            if (Objects.equals(oldShardAssignment, updatedShardAssignment)) {
                continue;
            }

            int numNewShardAssignments = updatedShardAssignment.assigned() - oldShardAssignment.assigned();
            // Shard copies could also be removed (decreased replica count), which would be a negative value we can disregard.
            numCopiesNewlyAssigned += Math.max(0, numNewShardAssignments);

            int numRemovedShardAssignments = oldShardAssignment.assigned() - updatedShardAssignment.assigned();
            // Shard copies could also be added (increased replica count), which would be a negative value we can disregard.
            numCopiesNewlyUnassigned += Math.max(0, numRemovedShardAssignments);

            // Now we need to check whether any of the shard copy assignments changed nodes.
            numCopiesMoved += shardMovements(oldShardAssignment, updatedShardAssignment);

            numCopiesMovedDueToShutdown += shardMovementsBecauseOfShutdown(oldShardAssignment, updatedShardAssignment, nodeIdsRemoved);
        }

        return new ShardMoveCounts(numCopiesMoved, numCopiesNewlyAssigned, numCopiesNewlyUnassigned, numCopiesMovedDueToShutdown);
    }

    /**
     * Returns the number of shard movements needed to reach the new shard assignment. Does not count shard copies going from unassigned to
     * assigned or assigned to unassigned: e.g. newly created or removed shards.
     */
    private static int shardMovements(ShardAssignment old, ShardAssignment updated) {
        // A shard move should retain the same number of assigned nodes, just swap out one node for another. We will compensate for newly
        // started shards -- adding a shard copy is not a move -- by initializing the count with a negative value so that incrementing later
        // for a new node zeros out.
        var movements = Math.min(0, old.assigned() - updated.assigned());
        for (String nodeId : updated.nodeIds()) {
            if (old.nodeIds().contains(nodeId) == false) {
                movements++;
            }
        }
        assert movements >= 0 : "Unexpected movement count [" + movements + "] between [" + old + "] and [" + updated + "]";
        return movements;
    }

    // Package visible for testing
    static int shardMovementsBecauseOfShutdown(ShardAssignment old, ShardAssignment updated, Set<String> shuttingDownNodeIds) {
        int countShuttingDownAndMoved = 0;
        for (String oldNodeId : old.nodeIds()) {
            // A node that is shutting down may not be able to move every shard elsewhere, so check that the updated assignment actually
            // moved the shard away (the update assignment should not include the node).
            // For example old: {N1, N2} -> updated {N1, N4}, where N1 & N2 are shutting down.
            if (shuttingDownNodeIds.contains(oldNodeId) && updated.nodeIds().contains(oldNodeId) == false) {
                ++countShuttingDownAndMoved;
            }
        }

        if (countShuttingDownAndMoved > 0) {
            // Perhaps a node is shutting down AND the replica count is being decreased at the same time. This could result in removal of a
            // shard copy instead of a move, like: old {N1, N3, N4} -> updated {N3, N5}, where N1 is shutting down.
            int diff = old.nodeIds().size() - updated.nodeIds().size();
            if (diff > 0) { // There are fewer assigned shards in the updated assignment
                // Any unassignments of shards will be considered as the shard(s) of shutting down node(s) being removed from assignment,
                // not movement. Make sure we don't go into negative, like if: old {N1, N3, N4} -> updated {N5}, where only N1 is shutting
                // down.
                countShuttingDownAndMoved = Math.max(0, countShuttingDownAndMoved - diff); // Make sure the value doesn't go negative.
            }
        }

        return countShuttingDownAndMoved;
    }

    public static String humanReadableDiff(DesiredBalance old, DesiredBalance updated) {
        var intersection = Sets.intersection(old.assignments().keySet(), updated.assignments().keySet());
        var diff = Sets.difference(Sets.union(old.assignments().keySet(), updated.assignments().keySet()), intersection);

        var newLine = System.lineSeparator();
        var builder = new StringBuilder();
        for (ShardId shardId : intersection) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            if (Objects.equals(oldAssignment, updatedAssignment) == false) {
                builder.append(newLine).append(shardId).append(": ").append(oldAssignment).append(" -> ").append(updatedAssignment);
            }
        }
        for (ShardId shardId : diff) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            builder.append(newLine).append(shardId).append(": ").append(oldAssignment).append(" -> ").append(updatedAssignment);
        }
        return builder.append(newLine).toString();
    }
}
