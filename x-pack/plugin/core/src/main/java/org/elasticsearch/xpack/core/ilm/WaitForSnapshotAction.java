/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which waits for snapshot to be taken (by configured SLM policy).
 */
public class WaitForSnapshotAction implements LifecycleAction {

    public static final String NAME = "wait_for_snapshot";
    public static final ParseField POLICY_FIELD = new ParseField("policy");

    private static final ConstructingObjectParser<WaitForSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new WaitForSnapshotAction((String) a[0])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_FIELD);
    }

    private final String policy;

    public static WaitForSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public WaitForSnapshotAction(String policy) {
        if (Strings.hasText(policy) == false) {
            throw new IllegalArgumentException("policy name must be specified");
        }
        this.policy = policy;
    }

    public WaitForSnapshotAction(StreamInput in) throws IOException {
        this(in.readString());
    }

    public String getPolicy() {
        return policy;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey waitForSnapshotKey = new StepKey(phase, NAME, WaitForSnapshotStep.NAME);
        return Collections.singletonList(new WaitForSnapshotStep(waitForSnapshotKey, nextStepKey, policy));
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
        WaitForSnapshotAction that = (WaitForSnapshotAction) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy);
    }
}
