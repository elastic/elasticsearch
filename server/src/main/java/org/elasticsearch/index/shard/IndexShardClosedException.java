/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class IndexShardClosedException extends IllegalIndexShardStateException {
    public IndexShardClosedException(ShardId shardId) {
        super(shardId, IndexShardState.CLOSED, "Closed");
    }

    public IndexShardClosedException(ShardId shardId, String message) {
        super(shardId, IndexShardState.CLOSED, message);
    }

    public IndexShardClosedException(StreamInput in) throws IOException {
        super(in);
    }
}
