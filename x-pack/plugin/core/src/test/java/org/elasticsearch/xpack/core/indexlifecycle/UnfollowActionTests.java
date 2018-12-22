/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class UnfollowActionTests extends AbstractActionTestCase<UnfollowAction> {

    @Override
    protected UnfollowAction doParseInstance(XContentParser parser) throws IOException {
        return UnfollowAction.parse(parser);
    }

    @Override
    protected UnfollowAction createTestInstance() {
        return new UnfollowAction();
    }

    @Override
    protected Reader<UnfollowAction> instanceReader() {
        return UnfollowAction::new;
    }

    public void testToSteps() {
        UnfollowAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps, notNullValue());
        assertThat(steps.size(), equalTo(3));

        StepKey expectedFirstStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForIndexingComplete.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForFollowShardTasksStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, UnfollowAction.NAME, UnfollowFollowIndexStep.NAME);

        WaitForIndexingComplete firstStep = (WaitForIndexingComplete) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(expectedSecondStepKey));

        WaitForFollowShardTasksStep secondStep = (WaitForFollowShardTasksStep) steps.get(1);
        assertThat(secondStep.getKey(), equalTo(expectedSecondStepKey));
        assertThat(secondStep.getNextStepKey(), equalTo(expectedThirdStepKey));

        UnfollowFollowIndexStep thirdStep = (UnfollowFollowIndexStep) steps.get(2);
        assertThat(thirdStep.getKey(), equalTo(expectedThirdStepKey));
        assertThat(thirdStep.getNextStepKey(), equalTo(nextStepKey));
    }
}
