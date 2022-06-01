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
import java.util.Set;

/**
 * The desired balance of the cluster, indicating which nodes should hold a copy of each shard.
 *
 * @param desiredAssignments a set of the (persistent) node IDs to which each {@link ShardId} should be allocated
 * @param unassigned a map of {@link  ShardId} that could not be assigned at the moment
 */
public record DesiredBalance(long lastConvergedIndex, Map<ShardId, Set<String>> desiredAssignments, Map<ShardId, Integer> unassigned) {
    public Set<String> getDesiredNodeIds(ShardId shardId) {
        return desiredAssignments.getOrDefault(shardId, Set.of());
    }

    public boolean isBalanceComputed(ShardId shardId) {
        return desiredAssignments.containsKey(shardId) || unassigned.containsKey(shardId);
    }

    public static boolean areSame(DesiredBalance a, DesiredBalance b) {
        return Objects.equals(a.desiredAssignments, b.desiredAssignments) && Objects.equals(a.unassigned, b.unassigned);
    }
}
