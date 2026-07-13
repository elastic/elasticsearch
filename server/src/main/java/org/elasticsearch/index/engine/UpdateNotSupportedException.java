/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class UpdateNotSupportedException extends EngineException {
    public UpdateNotSupportedException(ShardId shardId) {
        super(shardId, "Updates are not supported on indices with sequence numbers disabled");
    }

    public UpdateNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for indexing/updates; stack traces are expensive to compute
        // and not very useful for this exception, so don't fill it in.
        return this;
    }
}
