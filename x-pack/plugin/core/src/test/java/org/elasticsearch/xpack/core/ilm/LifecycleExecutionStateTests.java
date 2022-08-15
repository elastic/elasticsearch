/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
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
            LifecycleExecutionStateTests::mutate
        );
    }

    public void testGetCurrentStepKey() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        Step.StepKey stepKey = Step.getCurrentStepKey(lifecycleState.build());
        assertNull(stepKey);

        String phase = randomAlphaOfLength(20);
        String action = randomAlphaOfLength(20);
        String step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState2 = LifecycleExecutionState.builder();
        lifecycleState2.setPhase(phase);
        lifecycleState2.setAction(action);
        lifecycleState2.setStep(step);
        stepKey = Step.getCurrentStepKey(lifecycleState2.build());
        assertNotNull(stepKey);
        assertEquals(phase, stepKey.getPhase());
        assertEquals(action, stepKey.getAction());
        assertEquals(step, stepKey.getName());

        phase = randomAlphaOfLength(20);
        action = randomAlphaOfLength(20);
        step = null;
        LifecycleExecutionState.Builder lifecycleState3 = LifecycleExecutionState.builder();
        lifecycleState3.setPhase(phase);
        lifecycleState3.setAction(action);
        lifecycleState3.setStep(step);
        AssertionError error3 = expectThrows(AssertionError.class, () -> Step.getCurrentStepKey(lifecycleState3.build()));
        assertEquals("Current phase is not empty: " + phase, error3.getMessage());

        phase = null;
        action = randomAlphaOfLength(20);
        step = null;
        LifecycleExecutionState.Builder lifecycleState4 = LifecycleExecutionState.builder();
        lifecycleState4.setPhase(phase);
        lifecycleState4.setAction(action);
        lifecycleState4.setStep(step);
        AssertionError error4 = expectThrows(AssertionError.class, () -> Step.getCurrentStepKey(lifecycleState4.build()));
        assertEquals("Current action is not empty: " + action, error4.getMessage());

        phase = null;
        action = randomAlphaOfLength(20);
        step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState5 = LifecycleExecutionState.builder();
        lifecycleState5.setPhase(phase);
        lifecycleState5.setAction(action);
        lifecycleState5.setStep(step);
        AssertionError error5 = expectThrows(AssertionError.class, () -> Step.getCurrentStepKey(lifecycleState5.build()));
        assertNull(error5.getMessage());

        phase = null;
        action = null;
        step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState6 = LifecycleExecutionState.builder();
        lifecycleState6.setPhase(phase);
        lifecycleState6.setAction(action);
        lifecycleState6.setStep(step);
        AssertionError error6 = expectThrows(AssertionError.class, () -> Step.getCurrentStepKey(lifecycleState6.build()));
        assertNull(error6.getMessage());
    }

    private static LifecycleExecutionState mutate(LifecycleExecutionState toMutate) {
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(toMutate);
        switch (randomIntBetween(0, 17)) {
            case 0:
                newState.setPhase(randomValueOtherThan(toMutate.phase(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 1:
                newState.setAction(randomValueOtherThan(toMutate.action(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 2:
                newState.setStep(randomValueOtherThan(toMutate.step(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 3:
                newState.setPhaseDefinition(randomValueOtherThan(toMutate.phaseDefinition(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 4:
                newState.setFailedStep(randomValueOtherThan(toMutate.failedStep(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 5:
                newState.setStepInfo(randomValueOtherThan(toMutate.stepInfo(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 6:
                newState.setPhaseTime(randomValueOtherThan(toMutate.phaseTime(), ESTestCase::randomLong));
                break;
            case 7:
                newState.setActionTime(randomValueOtherThan(toMutate.actionTime(), ESTestCase::randomLong));
                break;
            case 8:
                newState.setStepTime(randomValueOtherThan(toMutate.stepTime(), ESTestCase::randomLong));
                break;
            case 9:
                newState.setIndexCreationDate(randomValueOtherThan(toMutate.lifecycleDate(), ESTestCase::randomLong));
                break;
            case 10:
                newState.setShrinkIndexName(randomValueOtherThan(toMutate.shrinkIndexName(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 11:
                newState.setSnapshotRepository(
                    randomValueOtherThan(toMutate.snapshotRepository(), () -> randomAlphaOfLengthBetween(5, 20))
                );
                break;
            case 12:
                newState.setSnapshotIndexName(randomValueOtherThan(toMutate.snapshotIndexName(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 13:
                newState.setSnapshotName(randomValueOtherThan(toMutate.snapshotName(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 14:
                newState.setRollupIndexName(randomValueOtherThan(toMutate.rollupIndexName(), () -> randomAlphaOfLengthBetween(5, 20)));
                break;
            case 15:
                newState.setIsAutoRetryableError(randomValueOtherThan(toMutate.isAutoRetryableError(), ESTestCase::randomBoolean));
                break;
            case 16:
                newState.setFailedStepRetryCount(randomValueOtherThan(toMutate.failedStepRetryCount(), ESTestCase::randomInt));
                break;
            case 17:
                return LifecycleExecutionState.builder().build();
            default:
                throw new IllegalStateException("unknown randomization branch");
        }
        return newState.build();
    }

    static Map<String, String> createCustomMetadata() {
        String phase = randomAlphaOfLengthBetween(5, 20);
        String action = randomAlphaOfLengthBetween(5, 20);
        String step = randomAlphaOfLengthBetween(5, 20);
        String failedStep = randomAlphaOfLengthBetween(5, 20);
        String stepInfo = randomAlphaOfLengthBetween(15, 50);
        String phaseDefinition = randomAlphaOfLengthBetween(15, 50);
        String repositoryName = randomAlphaOfLengthBetween(10, 20);
        String snapshotName = randomAlphaOfLengthBetween(10, 20);
        String snapshotIndexName = randomAlphaOfLengthBetween(10, 20);
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
        customMetadata.put("snapshot_repository", repositoryName);
        customMetadata.put("snapshot_name", snapshotName);
        customMetadata.put("snapshot_index_name", snapshotIndexName);
        customMetadata.put("shrink_index_name", randomAlphaOfLengthBetween(5, 20));
        customMetadata.put("rollup_index_name", randomAlphaOfLengthBetween(5, 20));
        customMetadata.put("is_auto_retryable_error", String.valueOf(randomBoolean()));
        customMetadata.put("failed_step_retry_count", String.valueOf(randomInt()));
        return customMetadata;
    }
}
