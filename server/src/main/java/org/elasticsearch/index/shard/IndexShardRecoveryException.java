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

import java.io.IOException;

public final class IndexShardRecoveryException extends ElasticsearchException {
    public IndexShardRecoveryException(ShardId shardId, String msg, Throwable cause) {
        super(msg, cause);
        setShard(shardId);
    }

    public IndexShardRecoveryException(StreamInput in) throws IOException {
        super(in);
    }
}
