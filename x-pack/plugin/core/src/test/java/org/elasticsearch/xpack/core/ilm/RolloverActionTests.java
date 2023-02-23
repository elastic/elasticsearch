/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;
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
        ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxPrimaryShardSize = randomBoolean()
            ? null
            : new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        Long maxPrimaryShardDocs = (maxSize == null && maxPrimaryShardSize == null && maxAge == null && maxDocs == null || randomBoolean())
            ? randomNonNegativeLong()
            : null;
        ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
        ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue minPrimaryShardSize = randomBoolean()
            ? null
            : new ByteSizeValue(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
        Long minDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue minAge = (minDocs == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        Long minPrimaryShardDocs = (minSize == null && minPrimaryShardSize == null && minAge == null && minDocs == null || randomBoolean())
            ? randomNonNegativeLong()
            : null;
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
        return RolloverAction::new;
    }

    @Override
    protected RolloverAction mutateInstance(RolloverAction instance) {
        RolloverConditions configuration = instance.getConditions();
        ByteSizeValue maxSize = configuration.getMaxSize();
        ByteSizeValue maxPrimaryShardSize = configuration.getMaxPrimaryShardSize();
        TimeValue maxAge = configuration.getMaxAge();
        Long maxDocs = configuration.getMaxDocs();
        Long maxPrimaryShardDocs = configuration.getMaxPrimaryShardDocs();
        ByteSizeValue minSize = configuration.getMinSize();
        ByteSizeValue minPrimaryShardSize = configuration.getMinPrimaryShardSize();
        TimeValue minAge = configuration.getMinAge();
        Long minDocs = configuration.getMinDocs();
        Long minPrimaryShardDocs = configuration.getMinPrimaryShardDocs();
        switch (between(0, 9)) {
            case 0 -> maxSize = randomValueOtherThan(maxSize, () -> {
                ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
            });
            case 1 -> maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> {
                ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
            });
            case 2 -> maxAge = randomValueOtherThan(
                maxAge,
                () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            );
            case 3 -> maxDocs = maxDocs == null ? randomNonNegativeLong() : maxDocs + 1;
            case 4 -> maxPrimaryShardDocs = maxPrimaryShardDocs == null ? randomNonNegativeLong() : maxPrimaryShardDocs + 1;
            case 5 -> minSize = randomValueOtherThan(minSize, () -> {
                ByteSizeUnit minSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / minSizeUnit.toBytes(1), minSizeUnit);
            });
            case 6 -> minPrimaryShardSize = randomValueOtherThan(minPrimaryShardSize, () -> {
                ByteSizeUnit minPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
                return new ByteSizeValue(randomNonNegativeLong() / minPrimaryShardSizeUnit.toBytes(1), minPrimaryShardSizeUnit);
            });
            case 7 -> minAge = randomValueOtherThan(
                minAge,
                () -> TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            );
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

    public void testToSteps() {
        RolloverAction action = createTestInstance();
        RolloverConditions configuration = action.getConditions();
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
        assertEquals(configuration, firstStep.getConditions());
        assertEquals(nextStepKey, fifthStep.getNextStepKey());
    }
}
