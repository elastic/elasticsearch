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

public class MlAutoscalingStatsTests extends AbstractWireSerializingTestCase<MlAutoscalingStats> {

    public static MlAutoscalingStats randomAutoscalingResources() {
        return new MlAutoscalingStats(
            randomIntBetween(0, 100), // nodes
            randomNonNegativeLong(), // memoryInBytesSum
            randomNonNegativeLong(), // modelMemoryInBytes
            randomIntBetween(0, 100), // minNodes
            randomNonNegativeLong(), // extraSingleNodeModelMemoryInBytes
            randomIntBetween(0, 100), // extraSingleNodeProcessors
            randomNonNegativeLong(), // extraModelMemoryInBytes
            randomIntBetween(0, 100), // extraProcessors
            randomNonNegativeLong(), // removeNodeMemoryInBytes
            randomNonNegativeLong() // perNodeMemoryOverheadInBytes
        );
    }

    @Override
    protected Writeable.Reader<MlAutoscalingStats> instanceReader() {
        return MlAutoscalingStats::new;
    }

    @Override
    protected MlAutoscalingStats createTestInstance() {
        return randomAutoscalingResources();
    }

    @Override
    protected MlAutoscalingStats mutateInstance(MlAutoscalingStats instance) throws IOException {
        return null; // TODO
    }
}
