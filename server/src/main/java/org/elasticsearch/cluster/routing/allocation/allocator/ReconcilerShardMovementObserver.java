/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;

/**
 * A {@link RoutingChangesObserver} that records shard movement metrics for the desired balance reconciler.
 * When used as a decorator of the allocation's observer during reconciliation, it maps routing change
 * callbacks to {@link DesiredBalanceMetrics.ShardMovementReason} and records them via {@link DesiredBalanceMetrics}.
 */
public class ReconcilerShardMovementObserver implements RoutingChangesObserver {

    private final DesiredBalanceMetrics desiredBalanceMetrics;

    public ReconcilerShardMovementObserver(DesiredBalanceMetrics desiredBalanceMetrics) {
        this.desiredBalanceMetrics = desiredBalanceMetrics;
    }

    @Override
    public void shardInitialized(ShardRouting unassignedShard, ShardRouting initializedShard) {
        desiredBalanceMetrics.recordReconcilerShardMovement(DesiredBalanceMetrics.ShardMovementReason.UNASSIGNED);
    }

    @Override
    public void relocationStarted(ShardRouting startedShard, ShardRouting targetRelocatingShard, String reason) {
        var movementReason = switch (reason) {
            case "move" -> DesiredBalanceMetrics.ShardMovementReason.CANNOT_REMAIN;
            case "rebalance" -> DesiredBalanceMetrics.ShardMovementReason.REBALANCE;
            default -> {
                assert false : "Unexpected relocation reason: " + reason;
                yield null;
            }
        };
        if (movementReason != null) {
            desiredBalanceMetrics.recordReconcilerShardMovement(movementReason);
        }
    }
}
