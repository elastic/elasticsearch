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

public class FreezeActionTests extends AbstractActionTestCase<FreezeAction> {

    @Override
    protected FreezeAction doParseInstance(XContentParser parser) throws IOException {
        return FreezeAction.parse(parser);
    }

    @Override
    protected FreezeAction createTestInstance() {
        return FreezeAction.INSTANCE;
    }

    @Override
    protected Reader<FreezeAction> instanceReader() {
        return in -> FreezeAction.INSTANCE;
    }

    public void testToSteps() {
        FreezeAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, FreezeAction.NAME, FreezeAction.CONDITIONAL_SKIP_FREEZE_STEP);
        StepKey expectedSecondStepKey = new StepKey(phase, FreezeAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, FreezeAction.NAME, FreezeStep.NAME);

        BranchingStep firstStep = (BranchingStep) steps.get(0);
        CheckNotDataStreamWriteIndexStep secondStep = (CheckNotDataStreamWriteIndexStep) steps.get(1);
        FreezeStep thirdStep = (FreezeStep) steps.get(2);

        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));

        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(expectedThirdStepKey, secondStep.getNextStepKey());
        assertEquals(expectedThirdStepKey, thirdStep.getKey());
        assertEquals(nextStepKey, thirdStep.getNextStepKey());
    }

    @Override
    protected void assertEqualInstances(FreezeAction expectedInstance, FreezeAction newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
