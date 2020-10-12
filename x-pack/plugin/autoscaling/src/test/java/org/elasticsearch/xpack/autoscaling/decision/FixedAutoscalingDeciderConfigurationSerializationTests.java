/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.decision;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.io.IOException;

public class FixedAutoscalingDeciderConfigurationSerializationTests extends AbstractSerializingTestCase<
    FixedAutoscalingDeciderConfiguration> {
    @Override
    protected FixedAutoscalingDeciderConfiguration doParseInstance(XContentParser parser) throws IOException {
        return FixedAutoscalingDeciderConfiguration.parse(parser);
    }

    @Override
    protected Writeable.Reader<FixedAutoscalingDeciderConfiguration> instanceReader() {
        return FixedAutoscalingDeciderConfiguration::new;
    }

    @Override
    protected FixedAutoscalingDeciderConfiguration createTestInstance() {
        return AutoscalingTestCase.randomFixedDecider();
    }

    @Override
    protected FixedAutoscalingDeciderConfiguration mutateInstance(FixedAutoscalingDeciderConfiguration instance) throws IOException {
        int parameter = randomInt(2);
        ByteSizeValue storage = instance.storage();
        ByteSizeValue memory = instance.memory();
        Integer nodes = instance.nodes();
        switch (parameter) {
            case 0:
                storage = randomValueOtherThan(storage, AutoscalingTestCase::randomNullableByteSizeValue);
                break;
            case 1:
                memory = randomValueOtherThan(memory, AutoscalingTestCase::randomNullableByteSizeValue);
                break;
            default:
                nodes = randomValueOtherThan(nodes, () -> randomBoolean() ? randomIntBetween(1, 1000) : null);
                break;
        }
        return new FixedAutoscalingDeciderConfiguration(storage, memory, nodes);
    }
}
