/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

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
        assertThat(steps.size(), equalTo(7));

        StepKey expectedFirstStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForIndexingCompleteStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForFollowShardTasksStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, UnfollowAction.NAME, PauseFollowerIndexStep.NAME);
        StepKey expectedFourthStepKey = new StepKey(phase, UnfollowAction.NAME, CloseFollowerIndexStep.NAME);
        StepKey expectedFifthStepKey = new StepKey(phase, UnfollowAction.NAME, UnfollowFollowIndexStep.NAME);
        StepKey expectedSixthStepKey = new StepKey(phase, UnfollowAction.NAME, OpenFollowerIndexStep.NAME);
        StepKey expectedSeventhStepKey = new StepKey(phase, UnfollowAction.NAME, WaitForYellowStep.NAME);

        WaitForIndexingCompleteStep firstStep = (WaitForIndexingCompleteStep) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(expectedSecondStepKey));

        WaitForFollowShardTasksStep secondStep = (WaitForFollowShardTasksStep) steps.get(1);
        assertThat(secondStep.getKey(), equalTo(expectedSecondStepKey));
        assertThat(secondStep.getNextStepKey(), equalTo(expectedThirdStepKey));

        PauseFollowerIndexStep thirdStep = (PauseFollowerIndexStep) steps.get(2);
        assertThat(thirdStep.getKey(), equalTo(expectedThirdStepKey));
        assertThat(thirdStep.getNextStepKey(), equalTo(expectedFourthStepKey));

        CloseFollowerIndexStep fourthStep = (CloseFollowerIndexStep) steps.get(3);
        assertThat(fourthStep.getKey(), equalTo(expectedFourthStepKey));
        assertThat(fourthStep.getNextStepKey(), equalTo(expectedFifthStepKey));

        UnfollowFollowIndexStep fifthStep = (UnfollowFollowIndexStep) steps.get(4);
        assertThat(fifthStep.getKey(), equalTo(expectedFifthStepKey));
        assertThat(fifthStep.getNextStepKey(), equalTo(expectedSixthStepKey));

        OpenFollowerIndexStep sixthStep = (OpenFollowerIndexStep) steps.get(5);
        assertThat(sixthStep.getKey(), equalTo(expectedSixthStepKey));
        assertThat(sixthStep.getNextStepKey(), equalTo(expectedSeventhStepKey));

        WaitForYellowStep seventhStep = (WaitForYellowStep) steps.get(6);
        assertThat(seventhStep.getKey(), equalTo(expectedSeventhStepKey));
        assertThat(seventhStep.getNextStepKey(), equalTo(nextStepKey));
    }
}
