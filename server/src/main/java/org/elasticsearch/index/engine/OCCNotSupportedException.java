/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class OCCNotSupportedException extends EngineException {
    /**
     * The transport version at which this exception type was introduced.
     */
    public static final TransportVersion OCC_NOT_SUPPORTED_EXCEPTION_VERSION = TransportVersion.fromName("occ_not_supported_exception");

    public OCCNotSupportedException(ShardId shardId) {
        super(shardId, "Optimistic concurrency control is not supported on indices with sequence numbers disabled");
    }

    public OCCNotSupportedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for indexing; stack traces are expensive to compute
        // and not very useful for this exception, so don't fill it in.
        return this;
    }
}
