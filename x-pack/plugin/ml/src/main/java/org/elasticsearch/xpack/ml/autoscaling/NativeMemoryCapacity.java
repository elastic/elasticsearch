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
public class NativeMemoryCapacity {

    static final NativeMemoryCapacity ZERO = new NativeMemoryCapacity(0L, 0L);

    static NativeMemoryCapacity from(NativeMemoryCapacity capacity) {
        return new NativeMemoryCapacity(capacity.tierMlNativeMemoryRequirement, capacity.nodeMlNativeMemoryRequirement, capacity.jvmSize);
    }

    private final long tierMlNativeMemoryRequirement;
    private final long nodeMlNativeMemoryRequirement;
    private final Long jvmSize;

    public NativeMemoryCapacity(long tierMlNativeMemoryRequirement, long nodeMlNativeMemoryRequirement, Long jvmSize) {
        this.tierMlNativeMemoryRequirement = tierMlNativeMemoryRequirement;
        this.nodeMlNativeMemoryRequirement = nodeMlNativeMemoryRequirement;
        this.jvmSize = jvmSize;
    }

    NativeMemoryCapacity(long tierMlNativeMemoryRequirement, long nodeMlNativeMemoryRequirement) {
        this.tierMlNativeMemoryRequirement = tierMlNativeMemoryRequirement;
        this.nodeMlNativeMemoryRequirement = nodeMlNativeMemoryRequirement;
        this.jvmSize = null;
    }

    /**
     * Merges the passed capacity with the current one. A new instance is created and returned
     * @param nativeMemoryCapacity the capacity to merge with
     * @return a new instance with the merged capacity values
     */
    NativeMemoryCapacity merge(final NativeMemoryCapacity nativeMemoryCapacity) {
        if (this == nativeMemoryCapacity) return this;
        long tier = this.tierMlNativeMemoryRequirement + nativeMemoryCapacity.tierMlNativeMemoryRequirement;
        long node = Math.max(nativeMemoryCapacity.nodeMlNativeMemoryRequirement, this.nodeMlNativeMemoryRequirement);
        // If the new node size is bigger, we have no way of knowing if the JVM size would stay the same
        // So null out
        Long jvmSize = nativeMemoryCapacity.nodeMlNativeMemoryRequirement > this.nodeMlNativeMemoryRequirement ? null : this.jvmSize;
        return new NativeMemoryCapacity(tier, node, jvmSize);
    }

    public AutoscalingCapacity autoscalingCapacity(int maxMemoryPercent, boolean useAuto) {
        // We calculate the JVM size here first to ensure it stays the same given the rest of the calculations
        final Long jvmSize = useAuto
            ? Optional.ofNullable(this.jvmSize).orElse(dynamicallyCalculateJvmSizeFromNativeMemorySize(nodeMlNativeMemoryRequirement))
            : null;
        // We first need to calculate the required node size given the required native ML memory size.
        // This way we can accurately determine the required node size AND what the overall memory percentage will be
        long requiredNodeSize = NativeMemoryCalculator.calculateApproxNecessaryNodeSize(
            nodeMlNativeMemoryRequirement,
            jvmSize,
            maxMemoryPercent,
            useAuto
        );
        // We make the assumption that the JVM size is the same across the entire tier
        // This simplifies calculating the tier as it means that each node in the tier
        // will have the same dynamic memory calculation. And thus the tier is simply the sum of the memory necessary
        // times that scaling factor.
        //
        // Since this is a _minimum_ node size, the memory percent calculated here is not
        // necessarily what "auto" will imply after scaling. Because the JVM occupies a
        // smaller proportion of memory the bigger the node, the memory percent might be
        // higher than we calculate here, potentially resulting in a bigger ML tier than
        // required. The effect is most pronounced when the minimum node size is small and
        // the minimum tier size is a lot bigger. For example, if the minimum node size is
        // 1GB and the total native ML memory requirement for the tier is 32GB then the memory
        // percent will be 41%, implying a minimum ML tier size of 78GB. But in reality a
        // single 64GB ML node would have an auto memory percent of 90%, and 90% of 64GB is
        // plenty big enough for the 32GB of ML native memory required.
        // TODO: improve this in the next refactoring
        double memoryPercentForMl = NativeMemoryCalculator.modelMemoryPercent(requiredNodeSize, jvmSize, maxMemoryPercent, useAuto);
        double inverseScale = memoryPercentForMl <= 0 ? 0 : 100.0 / memoryPercentForMl;
        long requiredTierSize = Math.round(Math.ceil(tierMlNativeMemoryRequirement * inverseScale));
        return new AutoscalingCapacity(
            // Tier should always be AT LEAST the largest node size.
            // This Math.max catches any strange rounding errors or weird input.
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes(Math.max(requiredTierSize, requiredNodeSize))),
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes(requiredNodeSize))
        );
    }

    public long getTierMlNativeMemoryRequirement() {
        return tierMlNativeMemoryRequirement;
    }

    public long getNodeMlNativeMemoryRequirement() {
        return nodeMlNativeMemoryRequirement;
    }

    public Long getJvmSize() {
        return jvmSize;
    }

    @Override
    public String toString() {
        return "NativeMemoryCapacity{"
            + "total ML native bytes="
            + ByteSizeValue.ofBytes(tierMlNativeMemoryRequirement)
            + ", largest node ML native bytes="
            + ByteSizeValue.ofBytes(nodeMlNativeMemoryRequirement)
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeMemoryCapacity that = (NativeMemoryCapacity) o;
        return tierMlNativeMemoryRequirement == that.tierMlNativeMemoryRequirement
            && nodeMlNativeMemoryRequirement == that.nodeMlNativeMemoryRequirement
            && Objects.equals(jvmSize, that.jvmSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierMlNativeMemoryRequirement, nodeMlNativeMemoryRequirement, jvmSize);
    }
}
