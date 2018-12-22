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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public final class UnfollowAction implements LifecycleAction {

    public static final String NAME = "unfollow";

    public UnfollowAction() {}

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey indexingComplete = new StepKey(phase, NAME, WaitForIndexingComplete.NAME);
        StepKey waitForFollowShardTasks = new StepKey(phase, NAME, WaitForFollowShardTasksStep.NAME);
        StepKey unfollowIndex = new StepKey(phase, NAME, UnfollowFollowIndexStep.NAME);

        WaitForIndexingComplete step1 = new WaitForIndexingComplete(indexingComplete, waitForFollowShardTasks);
        WaitForFollowShardTasksStep step2 = new WaitForFollowShardTasksStep(waitForFollowShardTasks, unfollowIndex, client);
        UnfollowFollowIndexStep  step3 = new UnfollowFollowIndexStep(unfollowIndex, nextStepKey, client);
        return Arrays.asList(step1, step2, step3);
    }

    @Override
    public List<StepKey> toStepKeys(String phase) {
        StepKey indexingCompleteStep = new StepKey(phase, NAME, WaitForIndexingComplete.NAME);
        StepKey waitForFollowShardTasksStep = new StepKey(phase, NAME, WaitForFollowShardTasksStep.NAME);
        StepKey unfollowIndexStep = new StepKey(phase, NAME, UnfollowFollowIndexStep.NAME);
        return Arrays.asList(indexingCompleteStep, waitForFollowShardTasksStep, unfollowIndexStep);
    }

    @Override
    public boolean isSafeAction() {
        // There are no settings to change, so therefor this action should be safe:
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public UnfollowAction(StreamInput in) throws IOException {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    private static final ObjectParser<UnfollowAction, Void> PARSER = new ObjectParser<>(NAME, UnfollowAction::new);

    public static UnfollowAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
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
