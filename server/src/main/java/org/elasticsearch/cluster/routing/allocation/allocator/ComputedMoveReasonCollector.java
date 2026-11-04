/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ComputedMoveReason;
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
 * Reasons recorded by the previous computation are carried forward while the move is still pending: when the computation starts by
 * relocating a copy to its previous target ({@link MoveReason#INHERITED}), or when the allocator re-decides the same move with the same
 * decider.
 */
final class ComputedMoveReasonCollector implements RoutingChangesObserver {

    private final RoutingNodes inputRoutingNodes;
    private final Map<ComputedMoveReason.Key, ComputedMoveReason> previousReasons;
    private final long inputIndex;
    private final Map<ComputedMoveReason.Key, ComputedMoveReason> reasons = new HashMap<>();

    ComputedMoveReasonCollector(
        RoutingNodes inputRoutingNodes,
        Map<ComputedMoveReason.Key, ComputedMoveReason> previousReasons,
        long inputIndex
    ) {
        this.inputRoutingNodes = inputRoutingNodes;
        this.previousReasons = previousReasons;
        this.inputIndex = inputIndex;
    }

    @Override
    public void relocationStarted(
        ShardRouting startedShard,
        ShardRouting targetRelocatingShard,
        MoveReason reason,
        @Nullable ComputedMoveReason computedReason
    ) {
        final var key = new ComputedMoveReason.Key(startedShard.shardId(), startedShard.currentNodeId());
        final var previous = previousReasons.get(key);
        switch (reason.type()) {
            case INHERITED -> {
                if (previous != null) {
                    reasons.put(key, previous.carried());
                }
            }
            case CANNOT_REMAIN, NOT_PREFERRED -> {
                if (wasOnSourceNodeInInput(startedShard) == false) {
                    return;
                }
                if (previous != null && previous.reason().equals(reason)) {
                    reasons.put(key, previous.carried());
                } else {
                    reasons.put(key, new ComputedMoveReason(reason, inputIndex, 0));
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
    Map<ComputedMoveReason.Key, ComputedMoveReason> finish(Map<ShardId, ShardAssignment> assignments) {
        reasons.keySet().removeIf(key -> {
            final var assignment = assignments.get(key.shardId());
            return assignment != null && assignment.nodeIds().contains(key.nodeId());
        });
        return Map.copyOf(reasons);
    }
}
