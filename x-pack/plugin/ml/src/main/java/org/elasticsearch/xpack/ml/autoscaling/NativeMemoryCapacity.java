/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingCapacity;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

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

    NativeMemoryCapacity(long tier, long node, Long jvmSize) {
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

    AutoscalingCapacity autoscalingCapacity(int maxMemoryPercent) {
        int memoryPercentForMl = jvmSize == null ?
            NativeMemoryCalculator.modelMemoryPercent(node, maxMemoryPercent) :
            NativeMemoryCalculator.modelMemoryPercent(node, jvmSize, maxMemoryPercent);
        double inverseScale = 100.0 / memoryPercentForMl;
        return new AutoscalingCapacity(
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes((long)Math.ceil(tier * inverseScale))),
            new AutoscalingCapacity.AutoscalingResources(null, ByteSizeValue.ofBytes((long)Math.ceil(node * inverseScale))));
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
}
