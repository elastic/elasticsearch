/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MockActionTests extends AbstractSerializingTestCase<MockAction> {

    @Override
    protected MockAction createTestInstance() {
        List<MockStep> steps = new ArrayList<>();
        int stepCount = randomIntBetween(2, 10);
        Step.StepKey currentStepKey = randomStepKey();
        Step.StepKey nextStepKey = null;
        for (int i = 0; i < stepCount - 1; i++) {
            nextStepKey = randomStepKey();
            steps.add(new MockStep(currentStepKey, nextStepKey));
            currentStepKey = nextStepKey;
        }
        steps.add(new MockStep(currentStepKey, null));
        return new MockAction(steps);
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
        List<MockStep> steps = new ArrayList<>(instance.getSteps());
        MockStep lastStep = steps.remove(steps.size() - 1);
        if (randomBoolean()) {
            Step.StepKey additionalStepKey = randomStepKey();
            steps.add(new MockStep(lastStep.getKey(), additionalStepKey));
            steps.add(new MockStep(additionalStepKey, null));
        }
        return new MockAction(steps);
    }

    private Step.StepKey randomStepKey() {
        return new Step.StepKey(randomAlphaOfLength(5),
            randomAlphaOfLength(5), randomAlphaOfLength(5));
    }
}

