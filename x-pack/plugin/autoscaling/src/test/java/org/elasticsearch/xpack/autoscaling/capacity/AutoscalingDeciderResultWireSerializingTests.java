/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

public class AutoscalingDeciderResultWireSerializingTests extends AbstractWireSerializingTestCase<AutoscalingDeciderResult> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected Writeable.Reader<AutoscalingDeciderResult> instanceReader() {
        return AutoscalingDeciderResult::new;
    }

    @Override
    protected AutoscalingDeciderResult createTestInstance() {
        return AutoscalingTestCase.randomAutoscalingDeciderResult();
    }

    @Override
    protected AutoscalingDeciderResult mutateInstance(AutoscalingDeciderResult instance) {
        AutoscalingCapacity requiredCapacity = instance.requiredCapacity();
        AutoscalingDeciderResult.Reason reason = instance.reason();
        switch (between(0, 1)) {
            case 0 -> requiredCapacity = randomValueOtherThan(
                requiredCapacity,
                () -> randomBoolean() ? null : AutoscalingTestCase.randomAutoscalingCapacity()
            );
            case 1 -> reason = randomValueOtherThan(reason, () -> AutoscalingTestCase.randomAutoscalingDeciderResult().reason());
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AutoscalingDeciderResult(requiredCapacity, reason);
    }

}
