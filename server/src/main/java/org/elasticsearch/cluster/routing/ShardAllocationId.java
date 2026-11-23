/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/// Identifies a single allocation of a shard to a node, by the shard and the ID of that allocation (see
/// [ShardRouting#allocationId()]). Keying bookkeeping by this pair, rather than by [ShardId] alone, keeps a
/// later allocation of the same shard (e.g. after a reroute, or a cancelled-then-retried allocation) from
/// being confused with an earlier one.
public record ShardAllocationId(ShardId shardId, String allocationId) implements Writeable {

    public ShardAllocationId(StreamInput in) throws IOException {
        this(new ShardId(in), in.readString());
    }

    /// Returns the key for the given routing entry's current allocation, which must be assigned to a node.
    public static ShardAllocationId fromRouting(ShardRouting routing) {
        assert routing.allocationId() != null : "no allocation ID for unassigned shard: " + routing;
        return new ShardAllocationId(routing.shardId(), routing.allocationId().getId());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeString(allocationId);
    }
}
