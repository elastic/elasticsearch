/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.io.IOException;
import java.util.Objects;

public class PutLifecycleRequest extends AcknowledgedRequest<PutLifecycleRequest> implements ToXContentObject {

    public interface Factory {
        PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy);

        String getPolicyName();
    }

    public static final ParseField POLICY_FIELD = new ParseField("policy");
    private static final ConstructingObjectParser<PutLifecycleRequest, Factory> PARSER = new ConstructingObjectParser<>(
        "put_lifecycle_request",
        false,
        (a, factory) -> factory.create((LifecyclePolicy) a[0])
    );

    static {
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (parser, factory) -> LifecyclePolicy.parse(parser, factory.getPolicyName()),
            POLICY_FIELD
        );
    }

    private final LifecyclePolicy policy;

    public PutLifecycleRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, LifecyclePolicy policy) {
        super(masterNodeTimeout, ackTimeout);
        this.policy = policy;
    }

    public PutLifecycleRequest(StreamInput in) throws IOException {
        super(in);
        policy = new LifecyclePolicy(in);
    }

    public LifecyclePolicy getPolicy() {
        return policy;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException err = null;
        try {
            this.policy.validate();
        } catch (IllegalArgumentException iae) {
            err = ValidateActions.addValidationError(iae.getMessage(), null);
        }
        String phaseTimingErr = TimeseriesLifecycleType.validateMonotonicallyIncreasingPhaseTimings(this.policy.getPhases().values());
        if (Strings.hasText(phaseTimingErr)) {
            err = new ActionRequestValidationException();
            err.addValidationError(phaseTimingErr);
        }
        return err;
    }

    public static PutLifecycleRequest parseRequest(Factory factory, XContentParser parser) {
        return PARSER.apply(parser, factory);
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
        PutLifecycleRequest other = (PutLifecycleRequest) obj;
        return Objects.equals(policy, other.policy);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
