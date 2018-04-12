/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class StepKeyTests extends ESTestCase {

    public StepKey createRandomInstance() {
        return new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public StepKey mutateInstance(StepKey instance) {
        String phase = instance.getPhase();
        String action = instance.getAction();
        String step = instance.getName();

        switch (between(0, 2)) {
        case 0:
            phase += randomAlphaOfLength(5);
            break;
        case 1:
            action += randomAlphaOfLength(5);
            break;
        case 2:
            step += randomAlphaOfLength(5);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new StepKey(phase, action, step);
    }

    public void testHashcodeAndEquals() {
        for (int runs = 0; runs < 20; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                    instance -> new StepKey(instance.getPhase(), instance.getAction(), instance.getName()), this::mutateInstance);
        }
    }
}
