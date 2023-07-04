/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.autoscaling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class AutoscalingResourcesTests extends AbstractWireSerializingTestCase<AutoscalingResources> {

    public static AutoscalingResources randomAutoscalingResources() {
        return new AutoscalingResources(
            randomIntBetween(0, 100), // nodes
            randomNonNegativeLong(), // memoryBytesSum
            randomNonNegativeLong(), // modelMemoryInBytes
            randomIntBetween(0, 100), // minNodes
            randomNonNegativeLong(), // extraSingleNodeModelMemoryInBytes
            randomIntBetween(0, 100), // extraSingleNodeProcessors
            randomNonNegativeLong(), // extraModelMemoryInBytes
            randomIntBetween(0, 100), // extraProcessors
            randomNonNegativeLong() // removeNodeMemoryInBytes
        );
    }

    @Override
    protected Writeable.Reader<AutoscalingResources> instanceReader() {
        return AutoscalingResources::new;
    }

    @Override
    protected AutoscalingResources createTestInstance() {
        return randomAutoscalingResources();
    }

    @Override
    protected AutoscalingResources mutateInstance(AutoscalingResources instance) throws IOException {
        return null; // TODO
    }
}
