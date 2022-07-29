/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.policy;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.mutateAutoscalingPolicy;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicyOfName;

public class AutoscalingPolicySerializingTests extends AbstractSerializingTestCase<AutoscalingPolicy> {

    private final String name = randomAlphaOfLength(8);

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return AutoscalingTestCase.getAutoscalingXContentRegistry();
    }

    @Override
    protected AutoscalingPolicy doParseInstance(final XContentParser parser) {
        return AutoscalingPolicy.parse(parser, name);
    }

    @Override
    protected Writeable.Reader<AutoscalingPolicy> instanceReader() {
        return AutoscalingPolicy::new;
    }

    @Override
    protected AutoscalingPolicy createTestInstance() {
        return randomAutoscalingPolicyOfName(name);
    }

    @Override
    protected AutoscalingPolicy mutateInstance(final AutoscalingPolicy instance) {
        return mutateAutoscalingPolicy(instance);
    }

}
