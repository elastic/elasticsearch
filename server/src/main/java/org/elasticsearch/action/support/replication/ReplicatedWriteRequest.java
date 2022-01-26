/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Requests that are both {@linkplain ReplicationRequest}s (run on a shard's primary first, then the replica) and {@linkplain WriteRequest}
 * (modify documents on a shard), for example {@link BulkShardRequest}, {@link IndexRequest}, and {@link DeleteRequest}.
 */
public abstract class ReplicatedWriteRequest<R extends ReplicatedWriteRequest<R>> extends ReplicationRequest<R> implements WriteRequest<R> {
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;

    /**
     * Constructor for thin deserialization.
     */
    public ReplicatedWriteRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        super(shardId, in);
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    /**
     * Constructor for deserialization.
     */
    public ReplicatedWriteRequest(StreamInput in) throws IOException {
        super(in);
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public ReplicatedWriteRequest(@Nullable ShardId shardId) {
        super(shardId);
    }

    @Override
    @SuppressWarnings("unchecked")
    public R setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return (R) this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        refreshPolicy.writeTo(out);
    }

    @Override
    public void writeThin(StreamOutput out) throws IOException {
        super.writeThin(out);
        refreshPolicy.writeTo(out);
    }
}
