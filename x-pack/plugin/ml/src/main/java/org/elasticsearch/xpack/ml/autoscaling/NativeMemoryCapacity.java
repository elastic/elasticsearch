/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator.dynamicallyCalculateJvmSizeFromNativeMemorySize;

// Used for storing native memory capacity and then transforming it into an autoscaling capacity
// which takes into account the whole node size
public class NativeMemoryCapacity  {

    static final NativeMemoryCapacity ZERO = new NativeMemoryCapacity(0L, 0L);

    static NativeMemoryCapacity from(NativeMemoryCapacity capacity) {
        return new NativeMemoryCapacity(capacity.tier, capacity.node, capacity.jvmSize);
    }

    private long tier;
    private long node;
    private Long jvmSize;

    public NativeMemoryCapacity(long tier, long node, Long jvmSize) {
        this.tier = tier;
        this.node = node;
        this.jvmSize = jvmSize;
    }

    NativeMemoryCapacity(long tier, long node) {
        this.tier = tier;
        this.node = node;
    }

    NativeMemoryCapacity merge(NativeMemoryCapacity nativeMemoryCapacity) {
        this.tier += nativeMemoryCapacity.tier;
        if (nativeMemoryCapacity.node > this.node) {
            this.node = nativeMemoryCapacity.node;
            // If the new node size is bigger, we have no way of knowing if the JVM size would stay the same
            // So null out
            this.jvmSize = null;
        }
        return this;
    }

    public AutoscalingCapacity autoscalingCapacity(int maxMemoryPercent, boolean useAuto) {
        // We calculate the JVM size here first to ensure it stays the same given the rest of the calculations
        final Long jvmSize = useAuto ?
            Optional.ofNullable(this.jvmSize).orElse(dynamicallyCalculateJvmSizeFromNativeMemorySize(node)) :
            null;
        // We first need to calculate the actual node size given the current native memory size.
        // This way we can accurately determine the required node size AND what the overall memory percentage will be
        long actualNodeSize = NativeMemoryCalculator.calculateApproxNecessaryNodeSize(node, jvmSize, maxMemoryPercent, useAuto);
        // We make the assumption that the JVM size is the same across the entire tier
        // This simplifies calculating the tier as it means that each node in the tier
        // will have the same dynamic memory calculation. And thus the tier is simply the sum of the memory necessary
        // times that scaling factor.
        double memoryPercentForMl = NativeMemoryCalculator.modelMemoryPercent(
            actualNodeSize,
            jvmSize,
            maxMemoryPercent,
            useAuto
        );
        double inverseScale = memoryPercentForMl <= 0 ? 0 : 100.0 / memoryPercentForMl;
        long actualTier = Math.round(tier * inverseScale);
        return new AutoscalingCapacity(
            // Tier should always be AT LEAST the largest node size.
            // This Math.max catches any strange rounding errors or weird input.
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes(Math.max(actualTier, actualNodeSize))),
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes(actualNodeSize))
        );
    }

    public long getTier() {
        return tier;
    }

    public long getNode() {
        return node;
    }

    public Long getJvmSize() {
        return jvmSize;
    }

    @Override
    public String toString() {
        return "NativeMemoryCapacity{" +
            "total bytes=" + ByteSizeValue.ofBytes(tier) +
            ", largest node bytes=" + ByteSizeValue.ofBytes(node) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeMemoryCapacity that = (NativeMemoryCapacity) o;
        return tier == that.tier && node == that.node && Objects.equals(jvmSize, that.jvmSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tier, node, jvmSize);
    }
}
