/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.io.IOException;
import java.util.Objects;

public class PutEnrichPolicyAction extends ActionType<AcknowledgedResponse> {

    public static final PutEnrichPolicyAction INSTANCE = new PutEnrichPolicyAction();
    public static final String NAME = "cluster:admin/xpack/enrich/put";

    private PutEnrichPolicyAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static Request fromXContent(XContentParser parser, String name) throws IOException {
        return new Request(name, EnrichPolicy.fromXContent(parser));
    }

    public static class Request extends MasterNodeRequest<PutEnrichPolicyAction.Request> {

        private final EnrichPolicy policy;
        private final String name;

        public Request(String name, EnrichPolicy policy) {
            this.name = Objects.requireNonNull(name, "name cannot be null");
            if (Version.CURRENT.equals(policy.getElasticsearchVersion()) == false) {
                throw new IllegalArgumentException("Cannot set [version_created] field on enrich policy [" + name +
                    "]. Found [" + policy.getElasticsearchVersion() + "]");
            }
            this.policy = policy;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
            policy = new EnrichPolicy(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            policy.writeTo(out);
        }

        public String getName() {
            return name;
        }

        public EnrichPolicy getPolicy() {
            return policy;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return policy.equals(request.policy) &&
                name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy, name);
        }
    }
}
