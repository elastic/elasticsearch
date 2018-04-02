/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class MockStep extends Step implements ToXContent, Writeable {

    public MockStep(StepKey stepKey, Step.StepKey nextStepKey) {
        super(stepKey, nextStepKey);
    }

    public MockStep(StreamInput in) throws IOException {
        super(new StepKey(in.readString(), in.readString(), in.readString()),
            new StepKey(in.readString(), in.readString(), in.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(getKey().getPhase());
        out.writeString(getKey().getAction());
        out.writeString(getKey().getName());
        out.writeString(getNextStepKey().getPhase());
        out.writeString(getNextStepKey().getAction());
        out.writeString(getNextStepKey().getName());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject("step_key");
            {
                builder.field("phase", getKey().getPhase());
                builder.field("action", getKey().getAction());
                builder.field("name", getKey().getName());
            }
            builder.endObject();
            builder.startObject("next_step_key");
            {
                builder.field("phase", getNextStepKey().getPhase());
                builder.field("action", getNextStepKey().getAction());
                builder.field("name", getNextStepKey().getName());
            }
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}