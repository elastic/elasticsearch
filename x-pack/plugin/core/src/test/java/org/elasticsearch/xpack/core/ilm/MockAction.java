/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MockAction implements LifecycleAction {
    public static final String NAME = "TEST_ACTION";
    private List<Step> steps;

    private static final ObjectParser<MockAction, Void> PARSER = new ObjectParser<>(NAME, MockAction::new);
    private final boolean safe;

    public static MockAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MockAction() {
        this(Collections.emptyList());
    }

    public MockAction(List<Step> steps) {
        this(steps, true);
    }

    public MockAction(List<Step> steps, boolean safe) {
        this.steps = steps;
        this.safe = safe;
    }

    public MockAction(StreamInput in) throws IOException {
        this.steps = in.readCollectionAsList(MockStep::new);
        this.safe = in.readBoolean();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<Step> getSteps() {
        return steps;
    }

    @Override
    public boolean isSafeAction() {
        return safe;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        return new ArrayList<>(steps);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(steps.stream().map(MockStep::new).collect(Collectors.toList()));
        out.writeBoolean(safe);
    }

    @Override
    public int hashCode() {
        return Objects.hash(steps, safe);
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
        return Objects.equals(steps, other.steps) && Objects.equals(safe, other.safe);
    }
}
