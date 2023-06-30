/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.rollup.RollupField;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class StartRollupJobAction extends ActionType<StartRollupJobAction.Response> {

    public static final StartRollupJobAction INSTANCE = new StartRollupJobAction();
    public static final String NAME = "cluster:admin/xpack/rollup/start";

    private StartRollupJobAction() {
        super(NAME, StartRollupJobAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> implements ToXContentObject {
        private String id;

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, RollupField.ID.getPreferredName());
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        public String getId() {
            return id;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RollupField.ID.getPreferredName(), id);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
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
            return Objects.equals(id, other.id);
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean started;

        public Response(boolean started) {
            super(Collections.emptyList(), Collections.emptyList());
            this.started = started;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            started = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(started);
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("started", started);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return started == response.started;
        }

        @Override
        public int hashCode() {
            return Objects.hash(started);
        }
    }
}
