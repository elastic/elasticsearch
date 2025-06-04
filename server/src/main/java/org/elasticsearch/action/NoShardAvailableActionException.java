/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.io.PrintWriter;

public final class NoShardAvailableActionException extends ElasticsearchException {

    // This is set so that no StackTrace is serialized in the scenario when we wrap other shard failures.
    // It isn't necessary to serialize this field over the wire as the empty stack trace is serialized instead.
    private final boolean onShardFailureWrapper;

    public static NoShardAvailableActionException forOnShardFailureWrapper(String msg) {
        return new NoShardAvailableActionException(null, msg, null, true);
    }

    public NoShardAvailableActionException(ShardId shardId) {
        this(shardId, null, null, false);
    }

    public NoShardAvailableActionException(ShardId shardId, String msg) {
        this(shardId, msg, null, false);
    }

    public NoShardAvailableActionException(ShardId shardId, String msg, Throwable cause) {
        this(shardId, msg, cause, false);
    }

    private NoShardAvailableActionException(ShardId shardId, String msg, Throwable cause, boolean onShardFailureWrapper) {
        super(msg, cause);
        setShard(shardId);
        this.onShardFailureWrapper = onShardFailureWrapper;
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }

    public NoShardAvailableActionException(StreamInput in) throws IOException {
        super(in);
        onShardFailureWrapper = false;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this; // this exception doesn't imply a bug, no need for a stack trace
    }

    @Override
    public void printStackTrace(PrintWriter s) {
        if (onShardFailureWrapper == false) {
            super.printStackTrace(s);
        } else {
            // Override to simply print the first line of the trace, which is the current exception.
            // Note: This will also omit the cause chain or any suppressed exceptions.
            s.println(this);
        }
    }
}
