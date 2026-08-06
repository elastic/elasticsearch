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

/**
 * A helper class that allows access to package private APIs for testing.
 */
public class ShardRoutingHelper {

    /// Shorthand for [#relocate(ShardRouting, String, long, ShardRouting.RecoveryPriority)] using a shard size of
    /// [ShardRouting#UNAVAILABLE_EXPECTED_SHARD_SIZE].
    public static ShardRouting relocate(ShardRouting routing, String nodeId, ShardRouting.RecoveryPriority recoveryPriority) {
        return routing.relocate(nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, recoveryPriority);
    }

    public static ShardRouting relocate(
        ShardRouting routing,
        String nodeId,
        long expectedByteSize,
        ShardRouting.RecoveryPriority recoveryPriority
    ) {
        return routing.relocate(nodeId, expectedByteSize, recoveryPriority);
    }

    public static ShardRouting moveToStarted(ShardRouting routing) {
        return routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public static ShardRouting moveToStarted(ShardRouting routing, long expectedShardSize) {
        return routing.moveToStarted(expectedShardSize);
    }

    public static ShardRouting initialize(ShardRouting routing, String nodeId) {
        return initialize(routing, nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public static ShardRouting initialize(ShardRouting routing, String nodeId, long expectedSize) {
        return routing.initialize(nodeId, null, expectedSize);
    }

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

    public static ShardRouting moveToUnassigned(ShardRouting routing, UnassignedInfo info, ShardRouting.RecoveryPriority recoveryPriority) {
        return routing.moveToUnassigned(info, recoveryPriority);
    }

    public static ShardRouting newWithRestoreSource(ShardRouting routing, SnapshotRecoverySource recoverySource) {
        return new ShardRouting(
            routing.shardId(),
            routing.currentNodeId(),
            routing.relocatingNodeId(),
            routing.primary(),
            routing.state(),
            recoverySource,
            switch (routing.state()) {
                // for testing, use arbitrary priority
                case INITIALIZING -> routing.relocatingNodeId() != null
                    ? ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                    : ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED;
                case UNASSIGNED -> ShardRouting.RecoveryPriority.UNASSIGNED_UNEXPECTED;
                case RELOCATING -> ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO;
                case STARTED -> null;
            },
            routing.unassignedInfo(),
            routing.relocationFailureInfo(),
            routing.allocationId(),
            routing.getExpectedShardSize(),
            routing.role()
        );
    }

    public static ShardRouting.RecoveryPriority recoveryPriorityForNewlyCreatedShard(boolean primary) {
        return primary ? ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY : ShardRouting.RecoveryPriority.UNASSIGNED_EXPECTED;
    }
}
