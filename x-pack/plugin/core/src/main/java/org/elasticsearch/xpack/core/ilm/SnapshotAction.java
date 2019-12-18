/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SnapshotAction implements LifecycleAction {

    public static final String NAME = "snapshot";
    public static final ParseField POLICY_FIELD = new ParseField("policy");

    private static final ConstructingObjectParser<SnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new SnapshotAction((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_FIELD);
    }

    private final String policy;

    public static SnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public SnapshotAction(String policy) {
        if (policy == null) {
            throw new IllegalArgumentException("Policy name must be specified");
        }
        this.policy = policy;
    }

    public SnapshotAction(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey waitForSnapshotKey = new StepKey(phase, NAME, WaitForSnapshotStep.NAME);
        return Collections.singletonList(new WaitForSnapshotStep(waitForSnapshotKey, nextStepKey, client, policy));
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(policy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY_FIELD.getPreferredName(), policy);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotAction that = (SnapshotAction) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy);
    }
}
