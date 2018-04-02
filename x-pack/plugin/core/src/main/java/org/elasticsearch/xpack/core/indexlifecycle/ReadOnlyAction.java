/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A {@link LifecycleAction} which force-merges the index.
 */
public class ReadOnlyAction implements LifecycleAction {
    public static final String NAME = "readonly";
    public static final ReadOnlyAction INSTANCE = new ReadOnlyAction();

    private static final ConstructingObjectParser<ReadOnlyAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> new ReadOnlyAction());

    public static ReadOnlyAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReadOnlyAction() {
    }

    public ReadOnlyAction(StreamInput in) {
    }

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
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Step.StepKey key = new Step.StepKey(phase, NAME, "readonly-step");
        return Collections.singletonList(new ReadOnlyStep(key, nextStepKey));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
