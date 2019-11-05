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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class GetEnrichPolicyAction extends ActionType<GetEnrichPolicyAction.Response> {

    public static final GetEnrichPolicyAction INSTANCE = new GetEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/get";

    private GetEnrichPolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final List<String> names;

        public Request() {
            this.names = new ArrayList<>();
        }

        public Request(String[] names) {
            this.names = Arrays.asList(names);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringList();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public List<String> getNames() {
            return names;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(names);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<EnrichPolicy.NamedPolicy> policies;

        public Response(Map<String, EnrichPolicy> policies) {
            Objects.requireNonNull(policies, "policies cannot be null");
            // use a treemap to guarantee ordering in the set, then transform it to the list of named policies
            this.policies = new TreeMap<>(policies).entrySet().stream()
                .map(entry -> new EnrichPolicy.NamedPolicy(entry.getKey(), entry.getValue())).collect(Collectors.toList());
        }

        public Response(StreamInput in) throws IOException {
            policies = in.readList(EnrichPolicy.NamedPolicy::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(policies);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startArray("policies");
                {
                    for (EnrichPolicy.NamedPolicy policy : policies) {
                        builder.startObject();
                        {
                            builder.startObject("config");
                            {
                                policy.toXContent(builder, params);
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            return builder;
        }

        public List<EnrichPolicy.NamedPolicy> getPolicies() {
            return policies;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return policies.equals(response.policies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policies);
        }
    }
}
