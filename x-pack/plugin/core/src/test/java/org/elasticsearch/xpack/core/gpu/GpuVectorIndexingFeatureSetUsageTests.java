/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.gpu;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

public class GpuVectorIndexingFeatureSetUsageTests extends AbstractWireSerializingTestCase<GpuVectorIndexingFeatureSetUsage> {

    @Override
    protected Writeable.Reader<GpuVectorIndexingFeatureSetUsage> instanceReader() {
        return GpuVectorIndexingFeatureSetUsage::new;
    }

    @Override
    protected GpuVectorIndexingFeatureSetUsage createTestInstance() {
        return randomUsage();
    }

    @Override
    protected GpuVectorIndexingFeatureSetUsage mutateInstance(GpuVectorIndexingFeatureSetUsage instance) {
        return randomValueOtherThan(instance, GpuVectorIndexingFeatureSetUsageTests::randomUsage);
    }

    static GpuVectorIndexingFeatureSetUsage randomUsage() {
        if (randomBoolean()) {
            // No GPU scenario
            return new GpuVectorIndexingFeatureSetUsage(false, false, 0, 0, List.of());
        }
        boolean available = randomBoolean();
        boolean enabled = randomBoolean();
        long indexBuildCount = randomLongBetween(0, 100_000);
        int nodesWithGpu = randomIntBetween(0, 10);
        List<GpuNodeStats> nodes = randomNodesList(nodesWithGpu);
        return new GpuVectorIndexingFeatureSetUsage(available, enabled, indexBuildCount, nodesWithGpu, nodes);
    }

    private static List<GpuNodeStats> randomNodesList(int count) {
        String[] gpuNames = { "NVIDIA L4", "NVIDIA A100", "NVIDIA H100", "NVIDIA T4", "NVIDIA V100" };
        List<GpuNodeStats> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(new GpuNodeStats(randomFrom(gpuNames), randomNonNegativeLong(), randomBoolean(), randomLongBetween(0, 10_000)));
        }
        return nodes;
    }
}
