/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

public class RolloverActionTests extends AbstractActionTestCase<RolloverAction> {

    @Override
    protected RolloverAction doParseInstance(XContentParser parser) throws IOException {
        return RolloverAction.parse(parser);
    }

    @Override
    protected RolloverAction createTestInstance() {
        return randomInstance();
    }

    public static RolloverAction randomInstance() {
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        return new RolloverAction(maxSize, maxAge, maxDocs);
    }

    @Override
    protected Reader<RolloverAction> instanceReader() {
        return RolloverAction::new;
    }

    @Override
    protected RolloverAction mutateInstance(RolloverAction instance) throws IOException {
        ByteSizeValue maxSize = instance.getMaxSize();
        TimeValue maxAge = instance.getMaxAge();
        Long maxDocs = instance.getMaxDocs();
        switch (between(0, 2)) {
            case 0:
                maxSize = randomValueOtherThan(maxSize, () -> {
                    ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                    return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
                });
                break;
            case 1:
                maxAge = randomValueOtherThan(maxAge,
                    () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test"));
                break;
            case 2:
                maxDocs = maxDocs == null ? randomNonNegativeLong() : maxDocs + 1;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new RolloverAction(maxSize, maxAge, maxDocs);
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new RolloverAction(null, null, null));
        assertEquals("At least one rollover condition must be set.", exception.getMessage());
    }

    public void testToSteps() {
        RolloverAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(5, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, RolloverAction.NAME, WaitForRolloverReadyStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, RolloverAction.NAME, RolloverStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, RolloverAction.NAME, WaitForActiveShardsStep.NAME);
        StepKey expectedFourthStepKey = new StepKey(phase, RolloverAction.NAME, UpdateRolloverLifecycleDateStep.NAME);
        StepKey expectedFifthStepKey = new StepKey(phase, RolloverAction.NAME, RolloverAction.INDEXING_COMPLETE_STEP_NAME);
        WaitForRolloverReadyStep firstStep = (WaitForRolloverReadyStep) steps.get(0);
        RolloverStep secondStep = (RolloverStep) steps.get(1);
        WaitForActiveShardsStep thirdStep = (WaitForActiveShardsStep) steps.get(2);
        UpdateRolloverLifecycleDateStep fourthStep = (UpdateRolloverLifecycleDateStep) steps.get(3);
        UpdateSettingsStep fifthStep = (UpdateSettingsStep) steps.get(4);
        assertEquals(expectedFirstStepKey, firstStep.getKey());
        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(expectedThirdStepKey, thirdStep.getKey());
        assertEquals(expectedFourthStepKey, fourthStep.getKey());
        assertEquals(expectedFifthStepKey, fifthStep.getKey());
        assertEquals(secondStep.getKey(), firstStep.getNextStepKey());
        assertEquals(thirdStep.getKey(), secondStep.getNextStepKey());
        assertEquals(fourthStep.getKey(), thirdStep.getNextStepKey());
        assertEquals(fifthStep.getKey(), fourthStep.getNextStepKey());
        assertEquals(action.getMaxSize(), firstStep.getMaxSize());
        assertEquals(action.getMaxAge(), firstStep.getMaxAge());
        assertEquals(action.getMaxDocs(), firstStep.getMaxDocs());
        assertEquals(nextStepKey, fifthStep.getNextStepKey());
    }
}
