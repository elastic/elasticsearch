/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.HashMap;
import java.util.Map;

public class LifecycleExecutionStateTests extends ESTestCase {

    public void testConversion() {
        Map<String, String> customMetadata = createCustomMetadata();
        LifecycleExecutionState parsed = LifecycleExecutionState.fromCustomMetadata(customMetadata);
        assertEquals(customMetadata, parsed.asMap());
    }

    public void testEmptyValuesAreNotSerialized() {
        LifecycleExecutionState empty = LifecycleExecutionState.builder().build();
        assertEquals(new HashMap<String, String>().entrySet(), empty.asMap().entrySet());

        Map<String, String> originalMap = createCustomMetadata();
        LifecycleExecutionState originalState = LifecycleExecutionState.fromCustomMetadata(originalMap);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(originalState);
        newState.setPhase(null);
        assertFalse(newState.build().asMap().containsKey("phase"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setAction(null);
        assertFalse(newState.build().asMap().containsKey("action"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setStep(null);
        assertFalse(newState.build().asMap().containsKey("step"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setFailedStep(null);
        assertFalse(newState.build().asMap().containsKey("failed_step"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setPhaseDefinition(null);
        assertFalse(newState.build().asMap().containsKey("phase_definition"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setStepInfo(null);
        assertFalse(newState.build().asMap().containsKey("step_info"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setPhaseTime(null);
        assertFalse(newState.build().asMap().containsKey("phase_time"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setActionTime(null);
        assertFalse(newState.build().asMap().containsKey("action_time"));

        newState = LifecycleExecutionState.builder(originalState);
        newState.setIndexCreationDate(null);
        assertFalse(newState.build().asMap().containsKey("creation_date"));
    }

    public void testEqualsAndHashcode() {
        LifecycleExecutionState original = LifecycleExecutionState.fromCustomMetadata(createCustomMetadata());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            original,
            toCopy -> LifecycleExecutionState.builder(toCopy).build(),
            LifecycleExecutionStateTests::mutate);
    }

    private static LifecycleExecutionState mutate(LifecycleExecutionState toMutate) {
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(toMutate);
        boolean changed = false;
        if (randomBoolean()) {
            newState.setPhase(randomValueOtherThan(toMutate.getPhase(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setAction(randomValueOtherThan(toMutate.getAction(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setStep(randomValueOtherThan(toMutate.getStep(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setPhaseDefinition(randomValueOtherThan(toMutate.getPhaseDefinition(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setFailedStep(randomValueOtherThan(toMutate.getFailedStep(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setStepInfo(randomValueOtherThan(toMutate.getStepInfo(), () -> randomAlphaOfLengthBetween(5, 20)));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setPhaseTime(randomValueOtherThan(toMutate.getPhaseTime(), ESTestCase::randomLong));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setActionTime(randomValueOtherThan(toMutate.getActionTime(), ESTestCase::randomLong));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setStepTime(randomValueOtherThan(toMutate.getStepTime(), ESTestCase::randomLong));
            changed = true;
        }
        if (randomBoolean()) {
            newState.setIndexCreationDate(randomValueOtherThan(toMutate.getLifecycleDate(), ESTestCase::randomLong));
            changed = true;
        }

        if (changed == false) {
            return LifecycleExecutionState.builder().build();
        }

        return newState.build();
    }

    static Map<String, String> createCustomMetadata() {
        String phase = randomAlphaOfLengthBetween(5,20);
        String action = randomAlphaOfLengthBetween(5,20);
        String step = randomAlphaOfLengthBetween(5,20);
        String failedStep = randomAlphaOfLengthBetween(5,20);
        String stepInfo = randomAlphaOfLengthBetween(15,50);
        String phaseDefinition = randomAlphaOfLengthBetween(15,50);
        long indexCreationDate = randomLong();
        long phaseTime = randomLong();
        long actionTime = randomLong();
        long stepTime = randomLong();

        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put("phase", phase);
        customMetadata.put("action", action);
        customMetadata.put("step", step);
        customMetadata.put("failed_step", failedStep);
        customMetadata.put("step_info", stepInfo);
        customMetadata.put("phase_definition", phaseDefinition);
        customMetadata.put("creation_date", String.valueOf(indexCreationDate));
        customMetadata.put("phase_time", String.valueOf(phaseTime));
        customMetadata.put("action_time", String.valueOf(actionTime));
        customMetadata.put("step_time", String.valueOf(stepTime));
        return customMetadata;
    }
}
