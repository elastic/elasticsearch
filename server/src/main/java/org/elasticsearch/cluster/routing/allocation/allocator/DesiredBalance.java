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
 * @param assignments a set of the (persistent) node IDs to which each {@link ShardId} should be allocated
 * @param weightsPerNode The node weights calculated based on
 * {@link org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction#nodeWeight}
 */
public record DesiredBalance(
    long lastConvergedIndex,
    Map<ShardId, ShardAssignment> assignments,
    Map<DiscoveryNode, DesiredBalanceMetrics.NodeWeightStats> weightsPerNode,
    ComputationFinishReason finishReason
) {

    enum ComputationFinishReason {
        CONVERGED,
        YIELD_TO_NEW_INPUT,
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

    private static int shardMovements(ShardAssignment old, ShardAssignment updated) {
        var movements = Math.min(0, old.assigned() - updated.assigned());// compensate newly started shards
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
