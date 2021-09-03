/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public abstract class BroadcastShardRequest extends TransportRequest implements IndicesRequest {

    private ShardId shardId;

    protected OriginalIndices originalIndices;

    protected BroadcastShardRequest() {}

    public BroadcastShardRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    protected BroadcastShardRequest(ShardId shardId, BroadcastRequest<? extends BroadcastRequest<?>> request) {
        this.shardId = shardId;
        this.originalIndices = new OriginalIndices(request);
    }

    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }
}
