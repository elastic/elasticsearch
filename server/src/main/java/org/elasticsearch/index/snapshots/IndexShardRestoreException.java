/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Generic shard restore exception
 */
public class IndexShardRestoreException extends ElasticsearchException {
    public IndexShardRestoreException(ShardId shardId, String msg) {
        this(shardId, msg, null);
    }

    @SuppressWarnings("this-escape")
    public IndexShardRestoreException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        setShard(shardId);
    }

    public IndexShardRestoreException(StreamInput in) throws IOException {
        super(in);
    }
}
