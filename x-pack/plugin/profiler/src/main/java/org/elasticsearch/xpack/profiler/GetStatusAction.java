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

        private boolean profilingEnabled;
        private boolean resourceManagementEnabled;
        private boolean resourcesCreated;

        public Response(StreamInput in) throws IOException {
            super(in);
            profilingEnabled = in.readBoolean();
            resourceManagementEnabled = in.readBoolean();
            resourcesCreated = in.readBoolean();
        }

        public Response(boolean profilingEnabled, boolean resourceManagementEnabled, boolean resourcesCreated) {
            this.profilingEnabled = profilingEnabled;
            this.resourceManagementEnabled = resourceManagementEnabled;
            this.resourcesCreated = resourcesCreated;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("profiling").field("enabled", profilingEnabled).endObject();
            builder.startObject("resource_management").field("enabled", resourceManagementEnabled).endObject();
            builder.startObject("resources").field("created", resourcesCreated).endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(profilingEnabled);
            out.writeBoolean(resourceManagementEnabled);
            out.writeBoolean(resourcesCreated);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return profilingEnabled == response.profilingEnabled
                && resourceManagementEnabled == response.resourceManagementEnabled
                && resourcesCreated == response.resourcesCreated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(profilingEnabled, resourceManagementEnabled, resourcesCreated);
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
