/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This exception defines illegal states of shard routing
 */
public class IllegalShardRoutingStateException extends RoutingException {

    private final ShardRouting shard;

    public IllegalShardRoutingStateException(ShardRouting shard, String message) {
        this(shard, message, null);
    }

    public IllegalShardRoutingStateException(ShardRouting shard, String message, Throwable cause) {
        super(shard.shortSummary() + ": " + message, cause);
        this.shard = shard;
    }

    public IllegalShardRoutingStateException(StreamInput in) throws IOException {
        super(in);
        shard = new ShardRouting(in);
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        shard.writeTo(out);
    }

    /**
     * Returns the shard instance referenced by this exception
     * @return shard instance referenced by this exception
     */
    public ShardRouting shard() {
        return shard;
    }
}
