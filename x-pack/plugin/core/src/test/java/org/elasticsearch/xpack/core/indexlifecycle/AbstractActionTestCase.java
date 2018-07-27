/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractActionTestCase<T extends LifecycleAction> extends AbstractSerializingTestCase<T> {

    public abstract void testToSteps();

    protected boolean isSafeAction() {
        return true;
    }

    public final void testIsSafeAction() {
        LifecycleAction action = createTestInstance();
        assertEquals(isSafeAction(), action.isSafeAction());
    }

    public void testToStepKeys() {
        T action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        List<StepKey> stepKeys = action.toStepKeys(phase);
        assertNotNull(stepKeys);
        List<StepKey> expectedStepKeys = steps.stream().map(Step::getKey).collect(Collectors.toList());
        assertEquals(expectedStepKeys, stepKeys);
    }
}
