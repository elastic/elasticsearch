/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toCollection;

public record ShardAssignment(Set<String> nodeIds, int total, int unassigned, int ignored) {

    public ShardAssignment {
        assert total > 0 : "Shard assignment should not be empty";
        assert nodeIds.size() + unassigned == total : "Shard assignment should account for all shards";
    }

    public boolean isIgnored(boolean primary) {
        return primary ? total == ignored : ignored > 0;
    }

    public int assigned() {
        return nodeIds.size();
    }

    public static ShardAssignment ofAssignedShards(List<ShardRouting> routings) {
        assert routings.stream().allMatch(ShardRouting::started) : routings;
        var nodeIds = routings.stream().map(ShardRouting::currentNodeId).collect(toCollection(LinkedHashSet::new));
        return new ShardAssignment(unmodifiableSet(nodeIds), routings.size(), 0, 0);
    }
}
