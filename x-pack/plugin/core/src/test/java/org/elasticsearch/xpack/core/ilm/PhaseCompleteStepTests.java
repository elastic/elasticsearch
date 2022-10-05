/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.xpack.core.ilm.Step.StepKey;

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
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
        }

        return new PhaseCompleteStep(key, nextKey);
    }

    @Override
    public PhaseCompleteStep copyInstance(PhaseCompleteStep instance) {
        return new PhaseCompleteStep(instance.getKey(), instance.getNextStepKey());
    }
}
