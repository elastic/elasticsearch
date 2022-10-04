/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.xpack.core.ilm.Step.StepKey;

public class TerminalPolicyStepTests extends AbstractStepTestCase<TerminalPolicyStep> {

    @Override
    public TerminalPolicyStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new TerminalPolicyStep(stepKey, nextStepKey);
    }

    @Override
    public TerminalPolicyStep mutateInstance(TerminalPolicyStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
        }

        return new TerminalPolicyStep(key, nextKey);
    }

    @Override
    public TerminalPolicyStep copyInstance(TerminalPolicyStep instance) {
        return new TerminalPolicyStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testInstance() {
        assertEquals(new Step.StepKey("completed", "completed", "completed"), TerminalPolicyStep.INSTANCE.getKey());
        assertNull(TerminalPolicyStep.INSTANCE.getNextStepKey());
    }
}
