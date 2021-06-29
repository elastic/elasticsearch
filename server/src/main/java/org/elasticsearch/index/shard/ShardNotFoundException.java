/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class ShardNotFoundException extends ResourceNotFoundException {
    public ShardNotFoundException(ShardId shardId) {
        this(shardId, null);
    }

    public ShardNotFoundException(ShardId shardId, Throwable ex) {
        this(shardId, "no such shard", ex);
    }

    public ShardNotFoundException(ShardId shardId, String msg, Object... args) {
        this(shardId, msg, null, args);
    }

    public ShardNotFoundException(ShardId shardId, String msg, Throwable ex, Object... args) {
        super(msg, ex, args);
        setShard(shardId);
    }

    public ShardNotFoundException(StreamInput in) throws IOException{
        super(in);
    }
}
