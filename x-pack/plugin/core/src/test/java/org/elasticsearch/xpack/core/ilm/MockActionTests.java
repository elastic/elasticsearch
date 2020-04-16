/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MockActionTests extends AbstractActionTestCase<MockAction> {

    @Override
    protected MockAction createTestInstance() {
        return new MockAction();
    }

    @Override
    protected MockAction doParseInstance(XContentParser parser) throws IOException {
        return MockAction.parse(parser);
    }

    @Override
    protected Reader<MockAction> instanceReader() {
        return MockAction::new;
    }

    @Override
    protected MockAction mutateInstance(MockAction instance) throws IOException {
        List<Step> steps = instance.getSteps();
        boolean safe = instance.isSafeAction();
        if (randomBoolean()) {
            steps = new ArrayList<>(steps);
            if (steps.size() > 0) {
                Step lastStep = steps.remove(steps.size() - 1);
                if (randomBoolean()) {
                    Step.StepKey additionalStepKey = randomStepKey();
                    steps.add(new MockStep(lastStep.getKey(), additionalStepKey));
                    steps.add(new MockStep(additionalStepKey, null));
                }
            } else {
                steps.add(new MockStep(randomStepKey(), null));
            }
        } else {
            safe = safe == false;
        }
        return new MockAction(steps, safe);
    }

    private static Step.StepKey randomStepKey() {
        return new Step.StepKey(randomAlphaOfLength(5),
            randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    @Override
    public void testToSteps() {
        int numSteps = randomIntBetween(1, 10);
        List<Step> steps = new ArrayList<>(numSteps);
        for (int i = 0; i < numSteps; i++) {
            steps.add(new MockStep(randomStepKey(), randomStepKey()));
        }
        MockAction action = new MockAction(steps);
        assertEquals(action.getSteps(), action.toSteps(null, null, null));
    }
}

