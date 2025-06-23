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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReadOnlyActionTests extends AbstractActionTestCase<ReadOnlyAction> {

    @Override
    protected ReadOnlyAction doParseInstance(XContentParser parser) {
        return ReadOnlyAction.parse(parser);
    }

    @Override
    protected ReadOnlyAction createTestInstance() {
        return new ReadOnlyAction();
    }

    @Override
    protected ReadOnlyAction mutateInstance(ReadOnlyAction instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<ReadOnlyAction> instanceReader() {
        return ReadOnlyAction::new;
    }

    public void testToSteps() {
        ReadOnlyAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, ReadOnlyAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, ReadOnlyAction.NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, ReadOnlyAction.NAME, ReadOnlyAction.NAME);
        CheckNotDataStreamWriteIndexStep firstStep = (CheckNotDataStreamWriteIndexStep) steps.get(0);
        WaitUntilTimeSeriesEndTimePassesStep secondStep = (WaitUntilTimeSeriesEndTimePassesStep) steps.get(1);
        ReadOnlyStep thirdStep = (ReadOnlyStep) steps.get(2);

        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(expectedSecondStepKey));

        assertThat(secondStep.getKey(), equalTo(expectedSecondStepKey));
        assertThat(secondStep.getNextStepKey(), equalTo(expectedThirdStepKey));

        assertThat(thirdStep.getKey(), equalTo(expectedThirdStepKey));
        assertThat(thirdStep.getNextStepKey(), equalTo(nextStepKey));
    }

}
