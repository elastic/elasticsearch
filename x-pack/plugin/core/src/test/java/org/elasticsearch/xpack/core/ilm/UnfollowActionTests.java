/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.OPEN_FOLLOWER_INDEX_STEP_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class UnfollowActionTests extends AbstractActionTestCase<UnfollowAction> {

    @Override
    protected UnfollowAction doParseInstance(XContentParser parser) throws IOException {
        return UnfollowAction.parse(parser);
    }

    @Override
    protected UnfollowAction createTestInstance() {
        return UnfollowAction.INSTANCE;
    }

    @Override
    protected Reader<UnfollowAction> instanceReader() {
        return in -> UnfollowAction.INSTANCE;
    }

    public void testToSteps() {
        UnfollowAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps, notNullValue());
        assertThat(steps.size(), equalTo(8));

        StepKey expectedFirstStepKey = new StepKey(phase, UnfollowAction.NAME, UnfollowAction.CONDITIONAL_UNFOLLOW_STEP);
        StepKey expectedSecondStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForIndexingCompleteStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForFollowShardTasksStep.NAME);
        StepKey expectedFourthStepKey = new StepKey(phase, UnfollowAction.NAME, PauseFollowerIndexStep.NAME);
        StepKey expectedFifthStepKey = new StepKey(phase, UnfollowAction.NAME, CloseFollowerIndexStep.NAME);
        StepKey expectedSixthStepKey = new StepKey(phase, UnfollowAction.NAME, UnfollowFollowerIndexStep.NAME);
        StepKey expectedSeventhStepKey = new StepKey(phase, UnfollowAction.NAME, OPEN_FOLLOWER_INDEX_STEP_NAME);
        StepKey expectedEighthStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForIndexColorStep.NAME);

        BranchingStep firstStep = (BranchingStep) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));

        WaitForIndexingCompleteStep secondStep = (WaitForIndexingCompleteStep) steps.get(1);
        assertThat(secondStep.getKey(), equalTo(expectedSecondStepKey));
        assertThat(secondStep.getNextStepKey(), equalTo(expectedThirdStepKey));

        WaitForFollowShardTasksStep thirdStep = (WaitForFollowShardTasksStep) steps.get(2);
        assertThat(thirdStep.getKey(), equalTo(expectedThirdStepKey));
        assertThat(thirdStep.getNextStepKey(), equalTo(expectedFourthStepKey));

        PauseFollowerIndexStep fourthStep = (PauseFollowerIndexStep) steps.get(3);
        assertThat(fourthStep.getKey(), equalTo(expectedFourthStepKey));
        assertThat(fourthStep.getNextStepKey(), equalTo(expectedFifthStepKey));

        CloseFollowerIndexStep fifthStep = (CloseFollowerIndexStep) steps.get(4);
        assertThat(fifthStep.getKey(), equalTo(expectedFifthStepKey));
        assertThat(fifthStep.getNextStepKey(), equalTo(expectedSixthStepKey));

        UnfollowFollowerIndexStep sixthStep = (UnfollowFollowerIndexStep) steps.get(5);
        assertThat(sixthStep.getKey(), equalTo(expectedSixthStepKey));
        assertThat(sixthStep.getNextStepKey(), equalTo(expectedSeventhStepKey));

        OpenIndexStep seventhStep = (OpenIndexStep) steps.get(6);
        assertThat(seventhStep.getKey(), equalTo(expectedSeventhStepKey));
        assertThat(seventhStep.getNextStepKey(), equalTo(expectedEighthStepKey));

        WaitForIndexColorStep eighthStep = (WaitForIndexColorStep) steps.get(7);
        assertThat(eighthStep.getColor(), is(ClusterHealthStatus.YELLOW));
        assertThat(eighthStep.getKey(), equalTo(expectedEighthStepKey));
        assertThat(eighthStep.getNextStepKey(), equalTo(nextStepKey));
    }

    @Override
    protected void assertEqualInstances(UnfollowAction expectedInstance, UnfollowAction newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
