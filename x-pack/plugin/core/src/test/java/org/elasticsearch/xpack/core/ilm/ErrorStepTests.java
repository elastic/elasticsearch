/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.xpack.core.ilm.Step.StepKey;

public class ErrorStepTests extends AbstractStepTestCase<ErrorStep> {

    @Override
    public ErrorStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), ErrorStep.NAME);
        return new ErrorStep(stepKey);
    }

    @Override
    public ErrorStep mutateInstance(ErrorStep instance) {
        StepKey key = instance.getKey();
        assertSame(instance.getNextStepKey(), instance.getKey());

        key = new StepKey(key.getPhase(), key.getAction() + randomAlphaOfLength(5), key.getName());

        return new ErrorStep(key);
    }

    @Override
    public ErrorStep copyInstance(ErrorStep instance) {
        assertSame(instance.getNextStepKey(), instance.getKey());
        return new ErrorStep(instance.getKey());
    }

    public void testInvalidStepKey() {
        StepKey invalidKey = randomStepKey();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new ErrorStep(invalidKey));
        assertEquals("An error step must have a step key whose step name is " + ErrorStep.NAME, exception.getMessage());
    }

    @Override
    public void testStepNameNotError() {
        // Need to override this test because this is the one special step that
        // is allowed to have ERROR as the step name
    }

}
