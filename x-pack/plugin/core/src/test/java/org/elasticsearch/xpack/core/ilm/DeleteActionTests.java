/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DeleteActionTests extends AbstractActionTestCase<DeleteAction> {

    @Override
    protected DeleteAction doParseInstance(XContentParser parser) throws IOException {
        return DeleteAction.parse(parser);
    }

    @Override
    protected DeleteAction createTestInstance() {
        return DeleteAction.WITH_SNAPSHOT_DELETE;
    }

    @Override
    protected DeleteAction mutateInstance(DeleteAction instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<DeleteAction> instanceReader() {
        return DeleteAction::readFrom;
    }

    public void testToSteps() {
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        {
            List<Step> steps = DeleteAction.WITH_SNAPSHOT_DELETE.toSteps(null, phase, nextStepKey);
            assertNotNull(steps);
            assertEquals(4, steps.size());
            StepKey expectedFirstStepKey = new StepKey(phase, DeleteAction.NAME, WaitForNoFollowersStep.NAME);
            StepKey expectedSecondStepKey = new StepKey(phase, DeleteAction.NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
            StepKey expectedThirdKey = new StepKey(phase, DeleteAction.NAME, CleanupSnapshotStep.NAME);
            StepKey expectedFourthKey = new StepKey(phase, DeleteAction.NAME, DeleteStep.NAME);
            WaitForNoFollowersStep firstStep = (WaitForNoFollowersStep) steps.get(0);
            WaitUntilTimeSeriesEndTimePassesStep secondStep = (WaitUntilTimeSeriesEndTimePassesStep) steps.get(1);
            CleanupSnapshotStep thirdStep = (CleanupSnapshotStep) steps.get(2);
            DeleteStep fourthStep = (DeleteStep) steps.get(3);
            assertEquals(expectedFirstStepKey, firstStep.getKey());
            assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
            assertEquals(expectedSecondStepKey, secondStep.getKey());
            assertEquals(expectedThirdKey, thirdStep.getKey());
            assertEquals(expectedFourthKey, thirdStep.getNextStepKey());
            assertEquals(expectedFourthKey, fourthStep.getKey());
            assertEquals(nextStepKey, fourthStep.getNextStepKey());
        }

        {
            List<Step> steps = DeleteAction.NO_SNAPSHOT_DELETE.toSteps(null, phase, nextStepKey);
            StepKey expectedFirstStepKey = new StepKey(phase, DeleteAction.NAME, WaitForNoFollowersStep.NAME);
            StepKey expectedSecondStepKey = new StepKey(phase, DeleteAction.NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
            StepKey expectedThirdStepKey = new StepKey(phase, DeleteAction.NAME, DeleteStep.NAME);
            assertEquals(3, steps.size());
            assertNotNull(steps);
            WaitForNoFollowersStep firstStep = (WaitForNoFollowersStep) steps.get(0);
            WaitUntilTimeSeriesEndTimePassesStep secondStep = (WaitUntilTimeSeriesEndTimePassesStep) steps.get(1);
            DeleteStep thirdStep = (DeleteStep) steps.get(2);
            assertEquals(expectedFirstStepKey, firstStep.getKey());
            assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
            assertEquals(expectedSecondStepKey, secondStep.getKey());
            assertEquals(expectedThirdStepKey, secondStep.getNextStepKey());
            assertEquals(expectedThirdStepKey, thirdStep.getKey());
            assertEquals(nextStepKey, thirdStep.getNextStepKey());
        }
    }

    @Override
    protected void assertEqualInstances(DeleteAction expectedInstance, DeleteAction newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
