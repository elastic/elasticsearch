/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;


import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class RollupAction extends ActionType<RollupAction.Response> {
    public static final RollupAction INSTANCE = new RollupAction();
    public static final String NAME = "indices:admin/xpack/rollup";

    private RollupAction() {
        super(NAME, RollupAction.Response::new);
    }

    public static class Request extends BroadcastRequest<Request> implements ToXContentObject {
        private String sourceIndex;
        private String rollupIndex;
        private RollupActionConfig rollupConfig;

        public Request(String sourceIndex, String rollupIndex, RollupActionConfig rollupConfig) {
            this.sourceIndex = sourceIndex;
            this.rollupIndex = rollupIndex;
            this.rollupConfig = rollupConfig;
        }

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            sourceIndex = in.readString();
            rollupIndex = in.readString();
            rollupConfig = new RollupActionConfig(in);
        }

        @Override
        public String[] indices() {
            return new String[] { sourceIndex };
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new RollupTask(id, type, action, parentTaskId, rollupIndex, rollupConfig, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeString(rollupIndex);
            rollupConfig.writeTo(out);
        }

        public String getSourceIndex() {
            return sourceIndex;
        }

        public String getRollupIndex() {
            return rollupIndex;
        }

        public RollupActionConfig getRollupConfig() {
            return rollupConfig;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("source_index", sourceIndex);
            builder.field("rollup_index", rollupIndex);
            rollupConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceIndex, rollupIndex, rollupConfig);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(sourceIndex, other.sourceIndex)
                && Objects.equals(rollupIndex, other.rollupIndex)
                && Objects.equals(rollupConfig, other.rollupConfig);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, RollupAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BroadcastResponse implements Writeable, ToXContentObject {
        private final boolean created;

        public Response(boolean created) {
            this.created = created;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            created = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(created);
        }

        public boolean isCreated() {
            return created;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("created", created);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return created == response.created;
        }

        @Override
        public int hashCode() {
            return Objects.hash(created);
        }
    }

    public static class ShardRequest extends BroadcastShardRequest {
        private final Request request;

        public ShardRequest(StreamInput in) throws IOException {
            super(in);
            this.request = new Request(in);
        }

        public ShardRequest(ShardId shardId, Request request) {
            super(shardId, request);
            this.request = request;
        }

        public String getRollupIndex() {
            return request.getRollupIndex();
        }

        public RollupActionConfig getRollupConfig() {
            return request.getRollupConfig();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class ShardResponse extends BroadcastShardResponse {
        public ShardResponse(StreamInput in) throws IOException {
            super(in);
        }

        public ShardResponse(ShardId shardId) {
            super(shardId);
        }
    }
}
