/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class NodePrevalidateShardPathResponse extends BaseNodeResponse {

    private final Set<ShardId> shardIds;

    protected NodePrevalidateShardPathResponse(DiscoveryNode node, Set<ShardId> shardIds) {
        super(node);
        this.shardIds = Set.copyOf(Objects.requireNonNull(shardIds));
    }

    protected NodePrevalidateShardPathResponse(StreamInput in) throws IOException {
        super(in);
        shardIds = in.readImmutableSet(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardIds);
    }

    public Set<ShardId> getShardIds() {
        return shardIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof NodePrevalidateShardPathResponse == false) return false;
        NodePrevalidateShardPathResponse other = (NodePrevalidateShardPathResponse) o;
        return Objects.equals(shardIds, other.shardIds) && Objects.equals(getNode(), other.getNode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardIds, getNode());
    }
}
