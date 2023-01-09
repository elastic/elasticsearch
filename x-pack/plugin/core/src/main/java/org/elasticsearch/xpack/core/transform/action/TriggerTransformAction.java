/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TriggerTransformAction extends ActionType<TriggerTransformAction.Response> {

    public static final TriggerTransformAction INSTANCE = new TriggerTransformAction();
    public static final String NAME = "cluster:admin/transform/trigger";

    private TriggerTransformAction() {
        super(NAME, TriggerTransformAction.Response::new);
    }

    public static class Request extends BaseTasksRequest<Request> {

        private final String id;

        public Request(String id, TimeValue timeout) {
            this.id = id;
            this.setTimeout(timeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
        }

        public static Request fromXContent(final String id, final TimeValue timeout) {
            return new Request(id, timeout);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getId() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
        }

        @Override
        public int hashCode() {
            // the base class does not implement hashCode, therefore we need to hash timeout ourselves
            return Objects.hash(getTimeout(), id);
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

            // the base class does not implement equals, therefore we need to check timeout ourselves
            return this.id.equals(other.id) && getTimeout().equals(other.getTimeout());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        public static final Response TRUE = new Response(true);

        private final boolean acknowledged;

        public Response(StreamInput in) throws IOException {
            super(in);
            acknowledged = in.readBoolean();
        }

        public Response(boolean acknowledged) {
            super(Collections.emptyList(), Collections.emptyList());
            this.acknowledged = acknowledged;
        }

        public Response(
            List<TaskOperationFailure> taskFailures,
            List<? extends ElasticsearchException> nodeFailures,
            boolean acknowledged
        ) {
            super(taskFailures, nodeFailures);
            this.acknowledged = acknowledged;
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            toXContentCommon(builder, params);
            builder.field("acknowledged", acknowledged);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TriggerTransformAction.Response response = (TriggerTransformAction.Response) o;
            return acknowledged == response.acknowledged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged);
        }
    }
}
