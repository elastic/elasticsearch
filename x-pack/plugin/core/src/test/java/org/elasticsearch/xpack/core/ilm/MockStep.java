/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class MockStep extends Step implements Writeable {
    public static final String NAME = "TEST_STEP";

    public MockStep(StepKey stepKey, Step.StepKey nextStepKey) {
        super(stepKey, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return false;
    }

    public MockStep(Step other) {
        super(other.getKey(), other.getNextStepKey());
    }

    public MockStep(StreamInput in) throws IOException {
        super(new StepKey(in.readString(), in.readString(), in.readString()), readOptionalNextStepKey(in));
    }

    private static StepKey readOptionalNextStepKey(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new StepKey(in.readString(), in.readString(), in.readString());
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getKey().phase());
        out.writeString(getKey().action());
        out.writeString(getKey().name());
        boolean hasNextStep = getNextStepKey() != null;
        out.writeBoolean(hasNextStep);
        if (hasNextStep) {
            out.writeString(getNextStepKey().phase());
            out.writeString(getNextStepKey().action());
            out.writeString(getNextStepKey().name());
        }
    }
}
