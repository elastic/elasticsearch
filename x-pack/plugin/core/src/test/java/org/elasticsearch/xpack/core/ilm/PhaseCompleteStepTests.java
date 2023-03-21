/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.hamcrest.Matchers.is;

public class PhaseCompleteStepTests extends AbstractStepTestCase<PhaseCompleteStep> {

    @Override
    public PhaseCompleteStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new PhaseCompleteStep(stepKey, nextStepKey);
    }

    @Override
    public PhaseCompleteStep mutateInstance(PhaseCompleteStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new PhaseCompleteStep(key, nextKey);
    }

    @Override
    public PhaseCompleteStep copyInstance(PhaseCompleteStep instance) {
        return new PhaseCompleteStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testPhaseCompeteStepKey() {
        String phaseName = randomAlphaOfLength(30);
        assertThat(PhaseCompleteStep.stepKey(phaseName), is(new StepKey(phaseName, PhaseCompleteStep.NAME, PhaseCompleteStep.NAME)));
    }
}
