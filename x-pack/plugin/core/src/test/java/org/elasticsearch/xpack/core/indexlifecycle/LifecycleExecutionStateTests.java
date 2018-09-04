/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class LifecycleExecutionStateTests extends ESTestCase {

    public void testConversion() {
        Map<String, String> customMetadata = createCustomMetadata();
        LifecycleExecutionState parsed = LifecycleExecutionState.fromCustomMetadata(customMetadata);
        assertEquals(customMetadata, parsed.asMap());
    }

    public void testInputValidation() {
        LifecycleExecutionState.Builder test = LifecycleExecutionState.builder();
        expectThrows(NullPointerException.class, () -> test.setPhase(null));
        expectThrows(NullPointerException.class, () -> test.setAction(null));
        expectThrows(NullPointerException.class, () -> test.setStep(null));
        expectThrows(NullPointerException.class, () -> test.setFailedStep(null));
        expectThrows(NullPointerException.class, () -> test.setStepInfo(null));
        expectThrows(NullPointerException.class, () -> test.setPhaseDefinition(null));
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
