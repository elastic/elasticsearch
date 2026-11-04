/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

/**
 * Why the desired balance computation decided that a shard copy should leave the node it is on. The computation works on simulated
 * state, so this may differ from the reason the reconciler finds when it eventually moves the copy.
 *
 * @param reason the computation's reason
 * @param originInputIndex the index of the desired balance input in which the computation first decided the copy should move
 * @param roundsCarried the number of later computations that have carried this reason forward while the move was pending
 */
public record ComputedMoveReason(MoveReason reason, long originInputIndex, int roundsCarried) {

    /**
     * Identifies a shard copy by its shard id and the node it is currently on.
     */
    public record Key(ShardId shardId, String nodeId) {}

    public ComputedMoveReason carried() {
        return new ComputedMoveReason(reason, originInputIndex, roundsCarried + 1);
    }
}
