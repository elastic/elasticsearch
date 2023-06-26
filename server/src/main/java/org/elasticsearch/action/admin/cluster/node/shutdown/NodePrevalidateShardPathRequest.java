/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * A node-specific request derived from the corresponding {@link PrevalidateShardPathRequest}.
*/
public class NodePrevalidateShardPathRequest extends TransportRequest {

    private final Set<ShardId> shardIds;

    public NodePrevalidateShardPathRequest(Collection<ShardId> shardIds) {
        this.shardIds = Set.copyOf(Objects.requireNonNull(shardIds));
    }

    public NodePrevalidateShardPathRequest(StreamInput in) throws IOException {
        super(in);
        this.shardIds = in.readImmutableSet(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardIds, (o, value) -> value.writeTo(o));
    }

    public Set<ShardId> getShardIds() {
        return shardIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof NodePrevalidateShardPathRequest == false) return false;
        NodePrevalidateShardPathRequest other = (NodePrevalidateShardPathRequest) o;
        return Objects.equals(shardIds, other.shardIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardIds);
    }
}
