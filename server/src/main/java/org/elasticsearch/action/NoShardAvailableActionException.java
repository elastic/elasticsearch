/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NoShardAvailableActionException extends ElasticsearchException {

    private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];

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
        if (in.getVersion().onOrAfter(Version.V_8_7_0)) {
            this.onShardFailureWrapper = in.readBoolean();
        } else {
            this.onShardFailureWrapper = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_8_7_0)) {
            out.writeBoolean(this.onShardFailureWrapper);
        }
    }

    @Override
    public StackTraceElement[] getStackTrace() {
        return onShardFailureWrapper ? EMPTY_STACK_TRACE : super.getStackTrace();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, String> delegatedParams = onShardFailureWrapper
            ? Map.of(REST_EXCEPTION_SKIP_STACK_TRACE, "true", REST_EXCEPTION_SKIP_CAUSE, "true")
            : Map.of();
        return super.toXContent(builder, new ToXContent.DelegatingMapParams(delegatedParams, params));
    }
}
