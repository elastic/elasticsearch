/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public abstract class BroadcastShardResponse extends TransportResponse {

    ShardId shardId;

    protected BroadcastShardResponse(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
    }

    protected BroadcastShardResponse(ShardId shardId) {
        this.shardId = shardId;
    }

    public String getIndex() {
        return this.shardId.getIndexName();
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
    }
}
