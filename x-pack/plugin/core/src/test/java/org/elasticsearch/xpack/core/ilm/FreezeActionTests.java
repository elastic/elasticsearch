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

public class FreezeActionTests extends AbstractActionTestCase<FreezeAction> {

    @Override
    protected FreezeAction doParseInstance(XContentParser parser) throws IOException {
        return FreezeAction.parse(parser);
    }

    @Override
    protected FreezeAction createTestInstance() {
        return new FreezeAction();
    }

    @Override
    protected Reader<FreezeAction> instanceReader() {
        return FreezeAction::new;
    }

    public void testToSteps() {
        FreezeAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(2, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, FreezeAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, FreezeAction.NAME, FreezeStep.NAME);

        CheckNotDataStreamWriteIndexStep firstStep = (CheckNotDataStreamWriteIndexStep) steps.get(0);
        FreezeStep secondStep = (FreezeStep) steps.get(1);

        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(expectedSecondStepKey));

        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(nextStepKey, secondStep.getNextStepKey());
    }
}
