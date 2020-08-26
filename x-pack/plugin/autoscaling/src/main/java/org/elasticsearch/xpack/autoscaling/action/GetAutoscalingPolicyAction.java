/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.io.IOException;
import java.util.Objects;

public class GetAutoscalingPolicyAction extends ActionType<GetAutoscalingPolicyAction.Response> {

    public static final GetAutoscalingPolicyAction INSTANCE = new GetAutoscalingPolicyAction();
    public static final String NAME = "cluster:admin/autoscaling/get_autoscaling_policy";

    private GetAutoscalingPolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final String name;

        public String name() {
            return name;
        }

        public Request(final String name) {
            this.name = Objects.requireNonNull(name);
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final AutoscalingPolicy policy;

        public AutoscalingPolicy policy() {
            return policy;
        }

        public Response(final AutoscalingPolicy policy) {
            this.policy = Objects.requireNonNull(policy);
        }

        public Response(final StreamInput in) throws IOException {
            policy = new AutoscalingPolicy(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            policy.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject();
            {
                builder.field("policy", policy);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Response response = (Response) o;
            return policy.equals(response.policy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy);
        }

    }

}
