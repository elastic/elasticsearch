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
        List<Step> steps = new ArrayList<>(instance.getSteps());
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
        return new MockAction(steps);
    }

    private static Step.StepKey randomStepKey() {
        return new Step.StepKey(randomAlphaOfLength(5),
            randomAlphaOfLength(5), randomAlphaOfLength(5));
    }
}

