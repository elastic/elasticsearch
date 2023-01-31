/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class UnpromotableShardRefreshRequest extends ActionRequest {

    private final ShardId shardId;
    private final long segmentGeneration;

    public UnpromotableShardRefreshRequest(final ShardId shardId, long segmentGeneration) {
        this.shardId = shardId;
        this.segmentGeneration = segmentGeneration;
    }

    public UnpromotableShardRefreshRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        segmentGeneration = in.readVLong();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeVLong(segmentGeneration);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getSegmentGeneration() {
        return segmentGeneration;
    }

    @Override
    public String toString() {
        return "UnpromotableShardRefreshRequest{" + "shardId=" + shardId + ", segmentGeneration=" + segmentGeneration + '}';
    }
}
