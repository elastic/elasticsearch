/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.io.IOException;
import java.util.Objects;

public class PutLifecycleAction extends ActionType<AcknowledgedResponse> {
    public static final PutLifecycleAction INSTANCE = new PutLifecycleAction();
    public static final String NAME = "cluster:admin/ilm/put";

    protected PutLifecycleAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ParseField POLICY_FIELD = new ParseField("policy");
        private static final ConstructingObjectParser<Request, String> PARSER = new ConstructingObjectParser<>(
            "put_lifecycle_request",
            a -> new Request((LifecyclePolicy) a[0])
        );
        static {
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), LifecyclePolicy::parse, POLICY_FIELD);
        }

        private LifecyclePolicy policy;

        public Request(LifecyclePolicy policy) {
            this.policy = policy;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            policy = new LifecyclePolicy(in);
        }

        public Request() {}

        public LifecyclePolicy getPolicy() {
            return policy;
        }

        @Override
        public ActionRequestValidationException validate() {
            this.policy.validate();
            ActionRequestValidationException err = null;
            String phaseTimingErr = TimeseriesLifecycleType.validateMonotonicallyIncreasingPhaseTimings(this.policy.getPhases().values());
            if (Strings.hasText(phaseTimingErr)) {
                err = new ActionRequestValidationException();
                err.addValidationError(phaseTimingErr);
            }
            return err;
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
