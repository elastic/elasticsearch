/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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
        // Ensure that at least one max* condition will be defined to produce a valid instance
        int useCondition = randomIntBetween(0, 4);
        ByteSizeValue maxSize = (useCondition == 0 || randomBoolean()) ? randomByteSizeValue() : null;
        ByteSizeValue maxPrimaryShardSize = (useCondition == 1 || randomBoolean()) ? randomByteSizeValue() : null;
        Long maxDocs = (useCondition == 2 || randomBoolean()) ? randomNonNegativeLong() : null;
        TimeValue maxAge = (useCondition == 3 || randomBoolean()) ? TimeValue.timeValueMillis(randomMillisUpToYear9999()) : null;
        Long maxPrimaryShardDocs = (useCondition == 4 || randomBoolean()) ? randomNonNegativeLong() : null;
        ByteSizeValue minSize = randomBoolean() ? randomByteSizeValue() : null;
        ByteSizeValue minPrimaryShardSize = randomBoolean() ? randomByteSizeValue() : null;
        Long minDocs = randomBoolean() ? randomNonNegativeLong() : null;
        TimeValue minAge = randomBoolean() ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test") : null;
        Long minPrimaryShardDocs = randomBoolean() ? randomNonNegativeLong() : null;
        return new RolloverAction(
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    @Override
    protected Reader<RolloverAction> instanceReader() {
        return RolloverAction::read;
    }

    @Override
    protected RolloverAction mutateInstance(RolloverAction instance) {
        RolloverConditions conditions = instance.getConditions();
        ByteSizeValue maxSize = conditions.getMaxSize();
        ByteSizeValue maxPrimaryShardSize = conditions.getMaxPrimaryShardSize();
        TimeValue maxAge = conditions.getMaxAge();
        Long maxDocs = conditions.getMaxDocs();
        Long maxPrimaryShardDocs = conditions.getMaxPrimaryShardDocs();
        ByteSizeValue minSize = conditions.getMinSize();
        ByteSizeValue minPrimaryShardSize = conditions.getMinPrimaryShardSize();
        TimeValue minAge = conditions.getMinAge();
        Long minDocs = conditions.getMinDocs();
        Long minPrimaryShardDocs = conditions.getMinPrimaryShardDocs();
        switch (between(0, 9)) {
            case 0 -> maxSize = randomValueOtherThan(maxSize, RolloverActionTests::randomByteSizeValue);
            case 1 -> maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, RolloverActionTests::randomByteSizeValue);
            case 2 -> maxAge = randomValueOtherThan(maxAge, () -> TimeValue.timeValueMillis(randomMillisUpToYear9999()));
            case 3 -> maxDocs = maxDocs == null ? randomNonNegativeLong() : maxDocs + 1;
            case 4 -> maxPrimaryShardDocs = maxPrimaryShardDocs == null ? randomNonNegativeLong() : maxPrimaryShardDocs + 1;
            case 5 -> minSize = randomValueOtherThan(minSize, RolloverActionTests::randomByteSizeValue);
            case 6 -> minPrimaryShardSize = randomValueOtherThan(minPrimaryShardSize, RolloverActionTests::randomByteSizeValue);
            case 7 -> minAge = randomValueOtherThan(minAge, () -> TimeValue.timeValueMillis(randomMillisUpToYear9999()));
            case 8 -> minDocs = minDocs == null ? randomNonNegativeLong() : minDocs + 1;
            case 9 -> minPrimaryShardDocs = minPrimaryShardDocs == null ? randomNonNegativeLong() : minPrimaryShardDocs + 1;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new RolloverAction(
            maxSize,
            maxPrimaryShardSize,
            maxAge,
            maxDocs,
            maxPrimaryShardDocs,
            minSize,
            minPrimaryShardSize,
            minAge,
            minDocs,
            minPrimaryShardDocs
        );
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RolloverAction(null, null, null, null, null, null, null, null, null, null)
        );
        assertEquals("At least one max_* rollover condition must be set.", exception.getMessage());
    }

    public void testToSteps() {
        RolloverAction action = createTestInstance();
        RolloverConditions conditions = action.getConditions();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
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
        assertEquals(conditions, firstStep.getConditions());
        assertEquals(nextStepKey, fifthStep.getNextStepKey());
    }

    public void testBwcSerializationWithMaxPrimaryShardDocs() throws Exception {
        // In case of serializing to node with older version, replace maxPrimaryShardDocs with maxDocs.
        RolloverAction instance = new RolloverAction(null, null, null, null, 1L, null, null, null, null, null);
        RolloverAction deserializedInstance = copyInstance(instance, TransportVersion.V_8_1_0);
        assertThat(deserializedInstance.getConditions().getMaxPrimaryShardDocs(), nullValue());

        // But not if maxDocs is also specified:
        instance = new RolloverAction(null, null, null, 2L, 1L, null, null, null, null, null);
        deserializedInstance = copyInstance(instance, TransportVersion.V_8_1_0);
        assertThat(deserializedInstance.getConditions().getMaxPrimaryShardDocs(), nullValue());
        assertThat(deserializedInstance.getConditions().getMaxDocs(), equalTo(instance.getConditions().getMaxDocs()));
    }
}
