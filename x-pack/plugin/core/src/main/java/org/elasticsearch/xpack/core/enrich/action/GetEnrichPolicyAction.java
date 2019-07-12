/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.Objects;

public class GetEnrichPolicyAction extends ActionType<GetEnrichPolicyAction.Response> {

    public static final GetEnrichPolicyAction INSTANCE = new GetEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/get";

    private GetEnrichPolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private String name;

        public Request(String name) {
            this.name = name;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readOptionalString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final EnrichPolicy policy;

        public Response(EnrichPolicy policy) {
            this.policy = Objects.requireNonNull(policy, "policy cannot be null");
        }

        public Response(StreamInput in) throws IOException {
            policy = new EnrichPolicy(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            policy.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                policy.toXContent(builder, params);
            }
            builder.endObject();

            return builder;
        }

        public EnrichPolicy getPolicy() {
            return policy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return policy.equals(response.policy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy);
        }
    }
}
