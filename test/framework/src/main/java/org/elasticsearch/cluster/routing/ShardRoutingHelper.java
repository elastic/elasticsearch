/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;

/**
 * A helper class that allows access to package private APIs for testing.
 */
public class ShardRoutingHelper {

    public static ShardRouting relocate(ShardRouting routing, String nodeId) {
        return relocate(routing, nodeId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public static ShardRouting relocate(ShardRouting routing, String nodeId, long expectedByteSize) {
        return routing.relocate(nodeId, expectedByteSize);
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
            new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, null),
            RelocationFailureInfo.NO_FAILURES,
            copy.allocationId(),
            copy.getExpectedShardSize(),
            copy.role()
        );
    }

    public static ShardRouting moveToUnassigned(ShardRouting routing, UnassignedInfo info) {
        return routing.moveToUnassigned(info);
    }

    public static ShardRouting newWithRestoreSource(ShardRouting routing, SnapshotRecoverySource recoverySource) {
        return new ShardRouting(
            routing.shardId(),
            routing.currentNodeId(),
            routing.relocatingNodeId(),
            routing.primary(),
            routing.state(),
            recoverySource,
            routing.unassignedInfo(),
            routing.relocationFailureInfo(),
            routing.allocationId(),
            routing.getExpectedShardSize(),
            routing.role()
        );
    }
}
