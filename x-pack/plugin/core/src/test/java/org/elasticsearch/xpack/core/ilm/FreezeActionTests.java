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
        assertEquals(1, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, FreezeAction.NAME, FreezeStep.NAME);
        FreezeStep firstStep = (FreezeStep) steps.get(0);
        assertEquals(expectedFirstStepKey, firstStep.getKey());
        assertEquals(nextStepKey, firstStep.getNextStepKey());
    }
}
