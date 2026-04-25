/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

/**
 * A {@link GPUMemoryService} that tracks manually how much memory is currently estimated to be used/available on a GPU.
 * This implementation is useful when we are not able to use a "Real memory" measurement; for example, if we are using pooled RMM memory,
 * the pool will permanently occupy most of the GPU RAM, allocations will happen inside the pool, and the "Real memory" measurement API
 * will always report a (tiny) fixed amount of free memory (whatever is not in the pool).
 */
class TrackingGPUMemoryService implements GPUMemoryService {

    private final long totalMemoryInBytes;
    private long availableMemoryInBytes;

    TrackingGPUMemoryService(long totalMemoryInBytes) {
        this.totalMemoryInBytes = totalMemoryInBytes;
        this.availableMemoryInBytes = totalMemoryInBytes;
    }

    @Override
    public long totalMemoryInBytes(CuVSResources res) {
        return totalMemoryInBytes;
    }

    @Override
    public long availableMemoryInBytes(CuVSResources res) {
        return availableMemoryInBytes;
    }

    @Override
    public void reserveMemory(long memoryInBytes) {
        availableMemoryInBytes -= memoryInBytes;
    }

    @Override
    public void releaseMemory(long memoryInBytes) {
        availableMemoryInBytes += memoryInBytes;
    }
}
