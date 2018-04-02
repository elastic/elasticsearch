/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MockAction implements LifecycleAction {
    public static final String NAME = "TEST_ACTION";
    private final List<MockStep> steps;

    private static final ConstructingObjectParser<MockAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new MockAction((List<MockStep>) a[0]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> p.list(),
            new ParseField("steps"), ObjectParser.ValueType.OBJECT_ARRAY);
    }

    public static MockAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MockAction(List<MockStep> steps) {
        this.steps = steps;
    }

    public MockAction(StreamInput in) throws IOException {
        this.steps = in.readList(MockStep::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("steps");
        for (MockStep step : steps) {
            step.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<MockStep> getSteps() {
        return steps;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        return new ArrayList<>(steps);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(steps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(steps);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        MockAction other = (MockAction) obj;
        return Objects.equals(steps, other.steps);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}