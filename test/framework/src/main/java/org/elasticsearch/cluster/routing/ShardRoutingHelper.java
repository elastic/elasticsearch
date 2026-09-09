/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;

/// A helper class providing shortcuts for various [ShardRouting] method and constructor calls.
public class ShardRoutingHelper {

    /// Shorthand for [ShardRouting#relocate] using an `expectedShardSize` of [ShardRouting#UNAVAILABLE_EXPECTED_SHARD_SIZE].
    public static ShardRouting relocate(ShardRouting routing, String nodeId, ShardRouting.RecoveryPriority recoveryPriority) {
        return routing.relocate(nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, recoveryPriority);
    }

    /// Shorthand for [ShardRouting#moveToStarted] using an `expectedShardSize` of [ShardRouting#UNAVAILABLE_EXPECTED_SHARD_SIZE].
    public static ShardRouting moveToStarted(ShardRouting routing) {
        return routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /// Shorthand for [ShardRouting#initialize] using a null `existingAllocationId` and an `expectedShardSize` of
    /// [ShardRouting#UNAVAILABLE_EXPECTED_SHARD_SIZE].
    public static ShardRouting initialize(ShardRouting routing, String nodeId) {
        return routing.initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /// Shorthand for [ShardRouting#initialize] using a null `existingAllocationId`.
    public static ShardRouting initialize(ShardRouting routing, String nodeId, long expectedShardSize) {
        return routing.initialize(nodeId, null, expectedShardSize);
    }

    /// Returns a copy of the given [ShardRouting] with its [ShardRoutingState] set to [ShardRoutingState#INITIALIZING], its
    /// [RecoverySource] set to the given value, and its [ShardRouting.RecoveryPriority], [UnassignedInfo], and [RelocationFailureInfo]
    /// adjusted for consistency.
    public static ShardRouting initWithSameId(ShardRouting copy, RecoverySource recoverySource) {
        return new ShardRouting(
            copy.shardId(),
            copy.currentNodeId(),
            copy.relocatingNodeId(),
            copy.primary(),
            ShardRoutingState.INITIALIZING,
            recoverySource,
            copy.relocatingNodeId() != null
                ? ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                : ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED, // for testing, use arbitrary allowed priority
            copy.relocatingNodeId() != null ? null : new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, null),
            RelocationFailureInfo.NO_FAILURES,
            copy.allocationId(),
            copy.getExpectedShardSize(),
            copy.role()
        );
    }

    /// Returns a copy of the given [ShardRouting] with its [RecoverySource] set to the given value.
    public static ShardRouting newWithRestoreSource(ShardRouting routing, SnapshotRecoverySource recoverySource) {
        return new ShardRouting(
            routing.shardId(),
            routing.currentNodeId(),
            routing.relocatingNodeId(),
            routing.primary(),
            routing.state(),
            recoverySource,
            routing.recoveryPriority(),
            routing.unassignedInfo(),
            routing.relocationFailureInfo(),
            routing.allocationId(),
            routing.getExpectedShardSize(),
            routing.role()
        );
    }

    /// Returns the [ShardRouting.RecoveryPriority] used for a shard newly created (as if by an API call).
    public static ShardRouting.RecoveryPriority recoveryPriorityForNewlyCreatedShard(boolean primary) {
        return primary ? ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY : ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED;
    }
}
