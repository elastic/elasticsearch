/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.gpu;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
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
        // indexBuildCount, nodesWithGpu, and nodes have no getters. Read them once, then return a new instance.
        UsageFields fields = readFields(instance);
        return switch (between(0, 4)) {
            case 0 -> new GpuVectorIndexingFeatureSetUsage(
                randomValueOtherThan(fields.available(), ESTestCase::randomBoolean),
                fields.enabled(),
                fields.indexBuildCount(),
                fields.nodesWithGpu(),
                fields.nodes()
            );
            case 1 -> new GpuVectorIndexingFeatureSetUsage(
                fields.available(),
                randomValueOtherThan(fields.enabled(), ESTestCase::randomBoolean),
                fields.indexBuildCount(),
                fields.nodesWithGpu(),
                fields.nodes()
            );
            case 2 -> new GpuVectorIndexingFeatureSetUsage(
                fields.available(),
                fields.enabled(),
                randomValueOtherThan(fields.indexBuildCount(), () -> randomLongBetween(0, 100_000)),
                fields.nodesWithGpu(),
                fields.nodes()
            );
            case 3 -> new GpuVectorIndexingFeatureSetUsage(
                fields.available(),
                fields.enabled(),
                fields.indexBuildCount(),
                randomValueOtherThan(fields.nodesWithGpu(), () -> randomIntBetween(0, 10)),
                fields.nodes()
            );
            default -> {
                if (fields.nodesWithGpu() == 0) {
                    // Empty list is the only list when count is 0; flip a different field instead.
                    yield new GpuVectorIndexingFeatureSetUsage(
                        randomValueOtherThan(fields.available(), ESTestCase::randomBoolean),
                        fields.enabled(),
                        fields.indexBuildCount(),
                        fields.nodesWithGpu(),
                        fields.nodes()
                    );
                }
                yield new GpuVectorIndexingFeatureSetUsage(
                    fields.available(),
                    fields.enabled(),
                    fields.indexBuildCount(),
                    fields.nodesWithGpu(),
                    randomValueOtherThan(fields.nodes(), () -> randomNodesList(fields.nodesWithGpu()))
                );
            }
        };
    }

    /**
     * {@link GpuVectorIndexingFeatureSetUsage} keeps the three stats private. {@code writeTo} is name, available,
     * enabled, then those stats, which is the same prefix {@link org.elasticsearch.xpack.core.XPackFeatureUsage} writes.
     */
    private static UsageFields readFields(GpuVectorIndexingFeatureSetUsage instance) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.readString();
                boolean available = in.readBoolean();
                boolean enabled = in.readBoolean();
                long indexBuildCount = in.readVLong();
                int nodesWithGpu = in.readVInt();
                List<GpuNodeStats> nodes = in.readCollectionAsList(GpuNodeStats::new);
                return new UsageFields(available, enabled, indexBuildCount, nodesWithGpu, nodes);
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private record UsageFields(boolean available, boolean enabled, long indexBuildCount, int nodesWithGpu, List<GpuNodeStats> nodes) {}

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
