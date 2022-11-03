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
import java.util.Set;

public class NodeCheckShardsOnDataPathRequest extends TransportRequest {

    private final Set<ShardId> shardIds;

    public NodeCheckShardsOnDataPathRequest(Collection<ShardId> shardIds) {
        this.shardIds = Set.copyOf(shardIds);
    }

    public NodeCheckShardsOnDataPathRequest(StreamInput in) throws IOException {
        super(in);
        this.shardIds = in.readSet(ShardId::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(shardIds, (o, value) -> value.writeTo(o));
    }

    public Set<ShardId> getShardIDs() {
        return shardIds;
    }

    public String getCustomDataPath() {
        return null; // TODO
    }

    // TODO: add timeout
}
