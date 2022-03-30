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
import java.util.Set;

/**
 * The desired balance of the cluster, indicating which nodes should hold a copy of each shard.
 *
 * @param desiredAssignments a list of the (persistent) node IDs to which each each {@link ShardId} should be allocated
 */
public record DesiredBalance(Map<ShardId, Set<String>> desiredAssignments) {
    public Set<String> getDesiredNodeIds(ShardId shardId) {
        return desiredAssignments.getOrDefault(shardId, Set.of());
    }
}
