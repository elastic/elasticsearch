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
        /** Computation ran to completion */
        CONVERGED,
        /** Computation exited and published early because a new cluster event occurred that affects computation */
        YIELD_TO_NEW_INPUT,
        /** Computation stopped and published early to avoid delaying new shard assignment */
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
        var intersection = Sets.intersection(old.assignments().keySet(), updated.assignments().keySet());
        int movements = 0;
        for (ShardId shardId : intersection) {
            var oldAssignment = old.getAssignment(shardId);
            var updatedAssignment = updated.getAssignment(shardId);
            if (Objects.equals(oldAssignment, updatedAssignment) == false) {
                movements += shardMovements(oldAssignment, updatedAssignment);
            }
        }
        return movements;
    }

    /**
     * Returns the number of shard movements needed to reach the new shard assignment. Doesn't count new shard copies as a move, nor removal
     * or unassignment of a shard copy.
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
