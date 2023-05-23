/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetStatusAction extends ActionType<GetStatusAction.Response> {
    public static final GetStatusAction INSTANCE = new GetStatusAction();
    public static final String NAME = "cluster:monitor/profiling/status/get";

    protected GetStatusAction() {
        super(NAME, GetStatusAction.Response::new);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean enabled;
        private boolean active;
        private boolean resourcesCreated;

        public Response(StreamInput in) throws IOException {
            super(in);
            enabled = in.readBoolean();
            active = in.readBoolean();
            resourcesCreated = in.readBoolean();
        }

        public Response(boolean enabled, boolean active, boolean resourcesCreated) {
            this.enabled = enabled;
            this.active = active;
            this.resourcesCreated = resourcesCreated;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("enabled", enabled);
            builder.field("active", active);
            builder.field("resources_created", resourcesCreated);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(enabled);
            out.writeBoolean(active);
            out.writeBoolean(resourcesCreated);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return enabled == response.enabled && active == response.active && resourcesCreated == response.resourcesCreated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled, active, resourcesCreated);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends AcknowledgedRequest<GetStatusAction.Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request() {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
