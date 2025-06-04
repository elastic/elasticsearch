/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Exception used when the in-memory lock for a shard cannot be obtained
 */
public final class ShardLockObtainFailedException extends ElasticsearchException {

    public ShardLockObtainFailedException(ShardId shardId, String message) {
        super(buildMessage(shardId, message));
        this.setShard(shardId);
    }

    public ShardLockObtainFailedException(ShardId shardId, String message, Throwable cause) {
        super(buildMessage(shardId, message), cause);
        this.setShard(shardId);
    }

    public ShardLockObtainFailedException(StreamInput in) throws IOException {
        super(in);
    }

    private static String buildMessage(ShardId shardId, String message) {
        return shardId.toString() + ": " + message;
    }
}
