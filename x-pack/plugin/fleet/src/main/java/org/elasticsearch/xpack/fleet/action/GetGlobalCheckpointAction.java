/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class GetGlobalCheckpointAction extends ActionType<GetGlobalCheckpointAction.Response> {

    public static final GetGlobalCheckpointAction INSTANCE = new GetGlobalCheckpointAction();
    // TODO: Do we still use xpack?
    public static final String NAME = "indices:monitor/xpack/fleet/global_checkpoint/";

    private GetGlobalCheckpointAction() {
        super(NAME, GetGlobalCheckpointAction.Response::new);
    }

    public static class Response extends ActionResponse {

        private final long globalCheckpoint;

        public Response(long globalCheckpoint) {
            this.globalCheckpoint = globalCheckpoint;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            globalCheckpoint = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(globalCheckpoint);
        }

        public long getGlobalCheckpoint() {
            return globalCheckpoint;
        }
    }

    public static class Request extends SingleShardRequest<Request> {

        private final ShardId shardId;
        private final boolean waitForAdvance;
        private final long currentCheckpoint;
        private final TimeValue pollTimeout;

        Request(ShardId shardId, boolean waitForAdvance, long currentCheckpoint, TimeValue pollTimeout) {
            this.shardId = shardId;
            this.waitForAdvance = waitForAdvance;
            this.currentCheckpoint = currentCheckpoint;
            this.pollTimeout = pollTimeout;
        }

        Request(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.waitForAdvance = in.readBoolean();
            this.currentCheckpoint = in.readLong();
            this.pollTimeout = in.readTimeValue();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId getShard() {
            return shardId;
        }

        public TimeValue pollTimeout() {
            return pollTimeout;
        }

        public boolean waitForAdvance() {
            return waitForAdvance;
        }

        // TODO: Name
        public long currentCheckpoint() {
            return currentCheckpoint;
        }
    }
}
