/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class MockStep extends Step implements ToXContent, Writeable {
    public static final String NAME = "TEST_STEP";
    static final ConstructingObjectParser<MockStep, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new MockStep((StepKey) a[0], (StepKey) a[1]));
    private static final ConstructingObjectParser<StepKey, Void> KEY_PARSER = new ConstructingObjectParser<>("TEST_KEY",
        a -> new StepKey((String) a[0], (String) a[1], (String) a[2]));

    static {
        KEY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("phase"));
        KEY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("action"));
        KEY_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), KEY_PARSER::apply,
            new ParseField("step_key"));
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), KEY_PARSER::apply,
            new ParseField("next_step_key"));
    }

    public MockStep(StepKey stepKey, Step.StepKey nextStepKey) {
        super(stepKey, nextStepKey);
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
        out.writeString(getKey().getPhase());
        out.writeString(getKey().getAction());
        out.writeString(getKey().getName());
        boolean hasNextStep = getNextStepKey() != null;
        out.writeBoolean(hasNextStep);
        if (hasNextStep) {
            out.writeString(getNextStepKey().getPhase());
            out.writeString(getNextStepKey().getAction());
            out.writeString(getNextStepKey().getName());
        }
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
            if (getNextStepKey() != null) {
                builder.startObject("next_step_key");
                {
                    builder.field("phase", getNextStepKey().getPhase());
                    builder.field("action", getNextStepKey().getAction());
                    builder.field("name", getNextStepKey().getName());
                }
                builder.endObject();
            }
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}