/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class DeleteAction implements LifecycleAction {
    public static final String NAME = "delete";

    private static final ObjectParser<DeleteAction, Void> PARSER = new ObjectParser<>(NAME, DeleteAction::new);

    public static DeleteAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public DeleteAction() {
    }

    public DeleteAction(StreamInput in) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Step.StepKey waitForNoFollowerStepKey = new Step.StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        Step.StepKey deleteStepKey = new Step.StepKey(phase, NAME, DeleteStep.NAME);

        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, deleteStepKey, client);
        DeleteStep deleteStep = new DeleteStep(deleteStepKey, nextStepKey, client);
        return Arrays.asList(waitForNoFollowersStep, deleteStep);
    }

    @Override
    public int hashCode() {
        return 1;
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
