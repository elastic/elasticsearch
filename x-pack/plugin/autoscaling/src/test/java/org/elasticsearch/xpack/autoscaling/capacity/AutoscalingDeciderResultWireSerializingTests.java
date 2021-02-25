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

}
