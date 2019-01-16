/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;

import java.io.IOException;
import java.util.Objects;

public class PutLifecycleAction extends Action<PutLifecycleAction.Response> {
    public static final PutLifecycleAction INSTANCE = new PutLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/put";

    protected PutLifecycleAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        public Response() {
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField POLICY_FIELD = new ParseField("policy");
        private static final ConstructingObjectParser<Request, String> PARSER =
            new ConstructingObjectParser<>("put_lifecycle_request", a -> new Request((LifecyclePolicy) a[0]));
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), LifecyclePolicy::parse, POLICY_FIELD);
        }

        private LifecyclePolicy policy;

        public Request(LifecyclePolicy policy) {
            this.policy = policy;
        }

        public Request() {
        }

        public LifecyclePolicy getPolicy() {
            return policy;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public static Request parseRequest(String name, XContentParser parser) {
            return PARSER.apply(parser, name);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(POLICY_FIELD.getPreferredName(), policy);
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            policy = new LifecyclePolicy(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            policy.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(policy, other.policy);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

}
