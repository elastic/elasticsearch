/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class ErrorStepTests extends AbstractStepTestCase<ErrorStep> {

    @Override
    public ErrorStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        return new ErrorStep(stepKey);
    }

    @Override
    public ErrorStep mutateInstance(ErrorStep instance) {
        StepKey key = instance.getKey();
        assertSame(instance.getNextStepKey(), instance.getKey());

        key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));

        return new ErrorStep(key);
    }

    @Override
    public ErrorStep copyInstance(ErrorStep instance) {
        assertSame(instance.getNextStepKey(), instance.getKey());
        return new ErrorStep(instance.getKey());
    }

}
