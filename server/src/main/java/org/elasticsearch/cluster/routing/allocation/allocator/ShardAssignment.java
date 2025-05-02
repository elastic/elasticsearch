/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

/**
 * Simple shard assignment summary of shard copies for a particular index shard.
 *
 * @param nodeIds The node IDs of nodes holding a shard copy.
 * @param total The total number of shard copies.
 * @param unassigned The number of unassigned shard copies.
 * @param ignored The number of ignored shard copies.
 */
public record ShardAssignment(Set<String> nodeIds, int total, int unassigned, int ignored) {

    public ShardAssignment {
        assert total > 0 : "Shard assignment should not be empty";
        assert nodeIds.size() + unassigned == total : "Shard assignment should account for all shards";
    }

    public int assigned() {
        return nodeIds.size();
    }

    /**
     * Helper method to instantiate a new ShardAssignment from a given list of ShardRouting instances. Assumes all shards are assigned.
     */
    public static ShardAssignment createFromAssignedShardRoutingsList(List<ShardRouting> routings) {
        var nodeIds = new LinkedHashSet<String>();
        for (ShardRouting routing : routings) {
            assert routing.unassignedInfo() == null : "Expected assigned shard copies only, unassigned info: " + routing.unassignedInfo();
            nodeIds.add(routing.currentNodeId());
        }
        return new ShardAssignment(unmodifiableSet(nodeIds), routings.size(), 0, 0);
    }

    @Override
    public String toString() {
        return "ShardAssignment{" + "nodeIds=" + nodeIds + ", total=" + total + ", unassigned=" + unassigned + ", ignored=" + ignored + '}';
    }
}
