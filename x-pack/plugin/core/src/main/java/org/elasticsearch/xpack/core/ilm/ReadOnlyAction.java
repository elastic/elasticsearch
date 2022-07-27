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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link LifecycleAction} which sets the index to be read-only.
 */
public class ReadOnlyAction implements LifecycleAction {
    public static final String NAME = "readonly";

    private static final ObjectParser<ReadOnlyAction, Void> PARSER = new ObjectParser<>(NAME, false, ReadOnlyAction::new);

    public static ReadOnlyAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReadOnlyAction() {}

    public ReadOnlyAction(StreamInput in) {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, NAME);
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex, readOnlyKey);
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, nextStepKey, client);
        return Arrays.asList(checkNotWriteIndexStep, readOnlyStep);
    }

    @Override
    public int hashCode() {
        return ReadOnlyAction.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
