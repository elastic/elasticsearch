/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class IllegalIndexShardStateException extends ElasticsearchException {

    private final IndexShardState currentState;

    public IllegalIndexShardStateException(ShardId shardId, IndexShardState currentState, String msg, Object... args) {
        this(shardId, currentState, msg, null, args);
    }

    public IllegalIndexShardStateException(ShardId shardId, IndexShardState currentState, String msg, Throwable ex, Object... args) {
        super("CurrentState[" + currentState + "] " + msg, ex, args);
        setShard(shardId);
        this.currentState = currentState;
    }

    public IndexShardState currentState() {
        return currentState;
    }

    public IllegalIndexShardStateException(StreamInput in) throws IOException {
        super(in);
        currentState = IndexShardState.fromId(in.readByte());
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeByte(currentState.id());
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }
}
