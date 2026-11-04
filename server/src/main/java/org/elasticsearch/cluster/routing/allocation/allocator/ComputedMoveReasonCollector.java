/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;

/**
 * Observes the relocations made during one desired balance computation and records, per shard copy, why the computation wanted the copy
 * to leave the node it is really on. Only moves triggered by a non-YES {@code canRemain} decision are recorded, and only when the copy
 * was on the move's source node in the computation's input (later hops of the same copy inside the simulation are not interesting).
 * Reasons recorded by the previous computation are kept while the move is still pending: when the computation starts by relocating a
 * copy to its previous target ({@link MoveReason#INHERITED}), or when the allocator re-decides the same move with the same decider.
 */
final class ComputedMoveReasonCollector implements RoutingChangesObserver {

    private final RoutingNodes inputRoutingNodes;
    private final Map<DesiredBalance.ShardCopy, MoveReason> previousReasons;
    private final Map<DesiredBalance.ShardCopy, MoveReason> reasons = new HashMap<>();

    ComputedMoveReasonCollector(RoutingNodes inputRoutingNodes, Map<DesiredBalance.ShardCopy, MoveReason> previousReasons) {
        this.inputRoutingNodes = inputRoutingNodes;
        this.previousReasons = previousReasons;
    }

    @Override
    public void relocationStarted(
        ShardRouting startedShard,
        ShardRouting targetRelocatingShard,
        MoveReason reason,
        @Nullable MoveReason computedReason
    ) {
        final var key = new DesiredBalance.ShardCopy(startedShard.shardId(), startedShard.currentNodeId());
        switch (reason.type()) {
            case INHERITED -> {
                // null when the previous computation moved the copy for a reason that is not recorded: rebalancing, an allocation
                // command, or a later hop of a copy that had already moved in the simulation
                final var previous = previousReasons.get(key);
                if (previous != null) {
                    reasons.put(key, previous);
                }
            }
            case CANNOT_REMAIN, NOT_PREFERRED -> {
                if (wasOnSourceNodeInInput(startedShard)) {
                    reasons.put(key, reason);
                }
            }
            case REBALANCE, COMMAND, RESTARTED -> {
                // not triggered by a canRemain decision
            }
        }
    }

    private boolean wasOnSourceNodeInInput(ShardRouting startedShard) {
        final var inputShard = inputRoutingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId());
        return inputShard != null && startedShard.currentNodeId().equals(inputShard.currentNodeId());
    }

    /**
     * @return the recorded reasons, minus those for copies the final assignment leaves where they are (the reconciler never looks
     *         those up)
     */
    Map<DesiredBalance.ShardCopy, MoveReason> finish(Map<ShardId, ShardAssignment> assignments) {
        reasons.keySet().removeIf(key -> {
            final var assignment = assignments.get(key.shardId());
            return assignment != null && assignment.nodeIds().contains(key.nodeId());
        });
        return Map.copyOf(reasons);
    }
}
