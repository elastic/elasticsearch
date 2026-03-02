/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface for GPU support detection and information.
 * <p>
 * Implementations should provide specific details about the GPU
 * support and capabilities.
 */
public interface GPUSupport {

    /** Tells whether the platform supports the use of GPUs. */
    boolean isSupported();

    /**
     * Returns the total device memory in bytes of the first available compatible GPU.
     *
     * @return total device memory in bytes, or 0 if GPU is not available or supported
     */
    long getTotalGpuMemory();

    /**
     * Returns the name of the first available compatible GPU, or null if no GPU is available.
     */
    String getGpuName();


    // TODO: move this to a more appropriate place (ad-hoc class? GPUPlugin?)
    AtomicLong GPU_USAGE_COUNT = new AtomicLong();

    /**
     * Increments the GPU index build count on this node.
     */
    static void incrementUsageCount() {
        GPU_USAGE_COUNT.incrementAndGet();
    }

    /**
     * Returns how many times GPU indexing was used on this node since it started.
     */
    static long getUsageCount() {
        return GPU_USAGE_COUNT.get();
    }
}
