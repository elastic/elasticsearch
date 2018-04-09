/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateActionTests extends AbstractSerializingTestCase<AllocateAction> {

    @Override
    protected AllocateAction doParseInstance(XContentParser parser) {
        return AllocateAction.parse(parser);
    }

    @Override
    protected AllocateAction createTestInstance() {
        boolean hasAtLeastOneMap = false;
        Map<String, String> includes;
        if (randomBoolean()) {
            includes = randomMap(1, 100);
            hasAtLeastOneMap = true;
        } else {
            includes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> excludes;
        if (randomBoolean()) {
            hasAtLeastOneMap = true;
            excludes = randomMap(1, 100);
        } else {
            excludes = randomBoolean() ? null : Collections.emptyMap();
        }
        Map<String, String> requires;
        if (hasAtLeastOneMap == false || randomBoolean()) {
            requires = randomMap(1, 100);
        } else {
            requires = randomBoolean() ? null : Collections.emptyMap();
        }
        return new AllocateAction(includes, excludes, requires);
    }

    @Override
    protected Reader<AllocateAction> instanceReader() {
        return AllocateAction::new;
    }

    @Override
    protected AllocateAction mutateInstance(AllocateAction instance) {
        Map<String, String> include = instance.getInclude();
        Map<String, String> exclude = instance.getExclude();
        Map<String, String> require = instance.getRequire();
        switch (randomIntBetween(0, 2)) {
        case 0:
            include = new HashMap<>(include);
            include.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 1:
            exclude = new HashMap<>(exclude);
            exclude.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        case 2:
            require = new HashMap<>(require);
            require.put(randomAlphaOfLengthBetween(11, 15), randomAlphaOfLengthBetween(1, 20));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new AllocateAction(include, exclude, require);
    }

    public void testAllMapsNullOrEmpty() {
        Map<String, String> include = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> exclude = randomBoolean() ? null : Collections.emptyMap();
        Map<String, String> require = randomBoolean() ? null : Collections.emptyMap();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new AllocateAction(include, exclude, require));
        assertEquals("At least one of " + AllocateAction.INCLUDE_FIELD.getPreferredName() + ", "
                + AllocateAction.EXCLUDE_FIELD.getPreferredName() + " or " + AllocateAction.REQUIRE_FIELD.getPreferredName()
                + "must contain attributes for action " + AllocateAction.NAME, exception.getMessage());
    }

    public static Map<String, String> randomMap(int minEntries, int maxEntries) {
        Map<String, String> map = new HashMap<>();
        int numIncludes = randomIntBetween(minEntries, maxEntries);
        for (int i = 0; i < numIncludes; i++) {
            map.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
        return map;
    }

    public void testToSteps() {
        AllocateAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, AllocateAction.NAME, EnoughShardsWaitStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, AllocateAction.NAME, UpdateAllocationSettingsStep.NAME);
        StepKey expectedThirdStepKey = new StepKey(phase, AllocateAction.NAME, AllocationRoutedStep.NAME);
        EnoughShardsWaitStep firstStep = (EnoughShardsWaitStep) steps.get(0);
        assertEquals(expectedFirstStepKey, firstStep.getKey());
        assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
        UpdateAllocationSettingsStep secondStep = (UpdateAllocationSettingsStep) steps.get(1);
        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(expectedThirdStepKey, secondStep.getNextStepKey());
        assertEquals(action.getInclude(), secondStep.getInclude());
        assertEquals(action.getExclude(), secondStep.getExclude());
        assertEquals(action.getRequire(), secondStep.getRequire());
        AllocationRoutedStep thirdStep = (AllocationRoutedStep) steps.get(2);
        assertEquals(expectedThirdStepKey, thirdStep.getKey());
        assertEquals(nextStepKey, thirdStep.getNextStepKey());
    }

}
