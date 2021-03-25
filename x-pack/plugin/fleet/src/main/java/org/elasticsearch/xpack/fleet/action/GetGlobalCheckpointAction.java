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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class GetGlobalCheckpointAction extends ActionType<GetGlobalCheckpointAction.Response> {

    public static final GetGlobalCheckpointAction INSTANCE = new GetGlobalCheckpointAction();
    // TODO: Do we still use xpack?
    public static final String NAME = "indices:monitor/xpack/fleet/global_checkpoints/";

    private GetGlobalCheckpointAction() {
        super(NAME, GetGlobalCheckpointAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final long[] globalCheckpoints;

        public Response(long[] globalCheckpoints) {
            this.globalCheckpoints = globalCheckpoints;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            globalCheckpoints = in.readLongArray();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLongArray(globalCheckpoints);
        }
    }

    public static class Request extends SingleShardRequest<Request> {

        Request(StreamInput in) throws IOException {

        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardId getShard() {
            return null;
        }

        public TimeValue getPollTimeout() {
            return null;
        }

        // TODO: Name
        public long getFromSeqNo() {
            return -1;
        }
    }
}
