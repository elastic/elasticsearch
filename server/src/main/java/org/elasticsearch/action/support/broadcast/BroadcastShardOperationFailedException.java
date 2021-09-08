/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.broadcast;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * An exception indicating that a failure occurred performing an operation on the shard.
 *
 *
 */
public class BroadcastShardOperationFailedException extends ElasticsearchException implements ElasticsearchWrapperException {

    public BroadcastShardOperationFailedException(ShardId shardId, String msg) {
        this(shardId, msg, null);
    }

    public BroadcastShardOperationFailedException(ShardId shardId, Throwable cause) {
        this(shardId, "", cause);
    }

    public BroadcastShardOperationFailedException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        setShard(shardId);
    }

    public BroadcastShardOperationFailedException(StreamInput in) throws IOException{
        super(in);
    }
}
