/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class PersistentSearchShardFetchFailure implements Writeable {
    private final ShardQueryResultInfo shardToReduce;
    private final Exception error;

    public PersistentSearchShardFetchFailure(ShardQueryResultInfo shardToReduce, Exception error) {
        this.shardToReduce = shardToReduce;
        this.error = error;
    }

    public PersistentSearchShardFetchFailure(StreamInput in) throws IOException {
        this.shardToReduce = new ShardQueryResultInfo(in);
        this.error = in.readException();
    }

    public ShardQueryResultInfo getShard() {
        return shardToReduce;
    }

    public Exception getError() {
        return error;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardToReduce.writeTo(out);
        out.writeException(error);
    }
}
