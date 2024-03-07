/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public record ShardAssignment(Set<String> nodeIds, int total, int unassigned, int ignored) implements Writeable {

    public ShardAssignment {
        assert total > 0 : "Shard assignment should not be empty";
        assert nodeIds.size() + unassigned == total : "Shard assignment should account for all shards";
    }

    public static ShardAssignment ofAssignedShards(List<ShardRouting> routings) {
        var nodeIds = new LinkedHashSet<String>();
        for (ShardRouting routing : routings) {
            nodeIds.add(routing.currentNodeId());
        }
        return new ShardAssignment(unmodifiableSet(nodeIds), routings.size(), 0, 0);
    }

    public static ShardAssignment readFrom(StreamInput in) throws IOException {
        return new ShardAssignment(in.readStringCollectionAsImmutableSet(), in.readVInt(), in.readVInt(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(nodeIds);
        out.writeVInt(total);
        out.writeVInt(unassigned);
        out.writeVInt(ignored);
    }

    public int assigned() {
        return nodeIds.size();
    }
}
