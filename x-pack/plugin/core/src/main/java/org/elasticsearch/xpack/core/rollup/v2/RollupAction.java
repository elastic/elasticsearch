/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.v2;


import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class RollupAction extends ActionType<RollupAction.Response> {

    public static final RollupAction INSTANCE = new RollupAction();
    public static final String NAME = "cluster:admin/xpack/rollupV2";

    private RollupAction() {
        super(NAME, RollupAction.Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {
        private RollupV2Config rollupConfig;

        public Request(RollupV2Config rollupConfig) {
            this.rollupConfig = rollupConfig;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            rollupConfig = new RollupV2Config(in);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new RollupTask(id, type, action, parentTaskId, rollupConfig, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            rollupConfig.writeTo(out);
        }

        public RollupV2Config getRollupConfig() {
            return rollupConfig;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            rollupConfig.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(rollupConfig);
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
            return Objects.equals(rollupConfig, other.rollupConfig);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, RollupAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements Writeable, ToXContentObject {

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
}
