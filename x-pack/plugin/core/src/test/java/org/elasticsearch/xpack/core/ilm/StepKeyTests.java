/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;


import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

public class StepKeyTests extends AbstractSerializingTestCase<StepKey> {

    @Override
    public StepKey createTestInstance() {
        return randomStepKey();
    }

    public static StepKey randomStepKey() {
        return new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<StepKey> instanceReader() {
        return StepKey::new;
    }

    @Override
    protected StepKey doParseInstance(XContentParser parser) {
        return StepKey.parse(parser);
    }

    @Override
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
}
