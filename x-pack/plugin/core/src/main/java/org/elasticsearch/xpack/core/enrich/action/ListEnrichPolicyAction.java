/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicyDefinition;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ListEnrichPolicyAction extends ActionType<ListEnrichPolicyAction.Response> {

    public static final ListEnrichPolicyAction INSTANCE = new ListEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/list";

    private ListEnrichPolicyAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<ListEnrichPolicyAction.Request> {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<EnrichPolicyDefinition.NamedPolicy> policies;

        public Response(Map<String, EnrichPolicyDefinition> policies) {
            Objects.requireNonNull(policies, "policies cannot be null");
            // use a treemap to guarantee ordering in the set, then transform it to the list of named policies
            this.policies = new TreeMap<>(policies).entrySet().stream()
                .map(entry -> new EnrichPolicyDefinition.NamedPolicy(entry.getKey(), entry.getValue())).collect(Collectors.toList());
        }

        public Response(StreamInput in) throws IOException {
            policies = in.readList(EnrichPolicyDefinition.NamedPolicy::new);
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
                    for (EnrichPolicyDefinition.NamedPolicy policy: policies) {
                        policy.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            builder.endObject();

            return builder;
        }

        public List<EnrichPolicyDefinition.NamedPolicy> getPolicies() {
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
