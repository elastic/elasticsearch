/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Objects;

/**
 * The desired balance of the cluster, indicating which nodes should hold a copy of each shard.
 *
 * @param assignments a set of the (persistent) node IDs to which each {@link ShardId} should be allocated
 */
public record DesiredBalance(long lastConvergedIndex, Map<ShardId, ShardAssignment> assignments) {

    public static final DesiredBalance INITIAL = new DesiredBalance(-1, Map.of());

    public ShardAssignment getAssignment(ShardId shardId) {
        return assignments.get(shardId);
    }

    public static boolean hasChanges(DesiredBalance a, DesiredBalance b) {
        return Objects.equals(a.assignments, b.assignments) == false;
    }

}
