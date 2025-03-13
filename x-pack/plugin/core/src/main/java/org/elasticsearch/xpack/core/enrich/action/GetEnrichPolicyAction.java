/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class GetEnrichPolicyAction extends ActionType<GetEnrichPolicyAction.Response> {

    public static final GetEnrichPolicyAction INSTANCE = new GetEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/get";

    private GetEnrichPolicyAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {

        private final List<String> names;

        public Request(TimeValue masterNodeTimeout, String... names) {
            super(masterNodeTimeout);
            this.names = List.of(names);
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringCollectionAsImmutableList();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        public List<String> getNames() {
            return names;
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
            this.policies = new TreeMap<>(policies).entrySet()
                .stream()
                .map(entry -> new EnrichPolicy.NamedPolicy(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(policies);
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
