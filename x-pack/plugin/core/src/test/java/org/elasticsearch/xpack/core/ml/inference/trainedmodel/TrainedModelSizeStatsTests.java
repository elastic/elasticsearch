/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

public class TrainedModelSizeStatsTests extends AbstractBWCWireSerializationTestCase<TrainedModelSizeStats> {

    @Override
    protected Writeable.Reader<TrainedModelSizeStats> instanceReader() {
        return TrainedModelSizeStats::new;
    }

    @Override
    protected TrainedModelSizeStats createTestInstance() {
        return createRandom();
    }

    @Override
    protected TrainedModelSizeStats mutateInstance(TrainedModelSizeStats instance) {
        long modelSizeBytes = instance.getModelSizeBytes();
        long requiredNativeMemoryBytes = instance.getRequiredNativeMemoryBytes();
        long runtimeNativeMemoryBytes = instance.getRuntimeNativeMemoryBytes();
        long peakRuntimeNativeMemoryBytes = instance.getPeakRuntimeNativeMemoryBytes();
        switch (between(0, 3)) {
            case 0 -> modelSizeBytes = randomValueOtherThan(modelSizeBytes, ESTestCase::randomNonNegativeLong);
            case 1 -> requiredNativeMemoryBytes = randomValueOtherThan(requiredNativeMemoryBytes, ESTestCase::randomNonNegativeLong);
            case 2 -> runtimeNativeMemoryBytes = randomValueOtherThan(runtimeNativeMemoryBytes, ESTestCase::randomNonNegativeLong);
            case 3 -> peakRuntimeNativeMemoryBytes = randomValueOtherThan(peakRuntimeNativeMemoryBytes, ESTestCase::randomNonNegativeLong);
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new TrainedModelSizeStats(modelSizeBytes, requiredNativeMemoryBytes, runtimeNativeMemoryBytes, peakRuntimeNativeMemoryBytes);
    }

    @Override
    protected TrainedModelSizeStats mutateInstanceForVersion(TrainedModelSizeStats instance, TransportVersion version) {
        if (version.supports(TrainedModelSizeStats.RUNTIME_NATIVE_MEMORY_STATS)) {
            return instance;
        }
        // The runtime/peak native memory fields are not sent to older nodes, so they read back as 0.
        return new TrainedModelSizeStats(instance.getModelSizeBytes(), instance.getRequiredNativeMemoryBytes(), 0L, 0L);
    }

    public static TrainedModelSizeStats createRandom() {
        return new TrainedModelSizeStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }
}
