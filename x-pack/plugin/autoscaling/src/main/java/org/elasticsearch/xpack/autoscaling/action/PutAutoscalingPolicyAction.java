/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;

import java.io.IOException;
import java.util.Objects;

public class PutAutoscalingPolicyAction extends ActionType<AcknowledgedResponse> {

    public static final PutAutoscalingPolicyAction INSTANCE = new PutAutoscalingPolicyAction();
    public static final String NAME = "cluster:admin/autoscaling/put_autoscaling_policy";

    private PutAutoscalingPolicyAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        static final ParseField POLICY_FIELD = new ParseField("policy");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_autoscaling_policy_request",
            a -> new Request((AutoscalingPolicy) a[0])
        );

        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), AutoscalingPolicy::parse, POLICY_FIELD);
        }

        public static Request parse(final XContentParser parser, final String name) {
            return PARSER.apply(parser, name);
        }

        private final AutoscalingPolicy policy;

        public AutoscalingPolicy policy() {
            return policy;
        }

        public Request(final AutoscalingPolicy policy) {
            this.policy = Objects.requireNonNull(policy);
        }

        public Request(final StreamInput in) throws IOException {
            super(in);
            policy = new AutoscalingPolicy(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            policy.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            // TODO: validate that the policy deciders are non-empty
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Request request = (Request) o;
            return policy.equals(request.policy);
        }

        @Override
        public int hashCode() {
            return Objects.hash(policy);
        }

    }

}
