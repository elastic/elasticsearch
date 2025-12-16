/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.GPUInfoProvider;

/**
 * A {@link GPUMemoryService} that tracks how much memory is currently used/available on a GPU by using the GPU free/total memory APIs
 * (via a {@link GPUInfoProvider})
 */
class RealGPUMemoryService implements GPUMemoryService {
    private final GPUInfoProvider gpuInfoProvider;

    RealGPUMemoryService(GPUInfoProvider gpuInfoProvider) {
        this.gpuInfoProvider = gpuInfoProvider;
    }

    @Override
    public long totalMemoryInBytes(CuVSResources res) {
        return gpuInfoProvider.getCurrentInfo(res).totalDeviceMemoryInBytes();
    }

    @Override
    public long availableMemoryInBytes(CuVSResources res) {
        return gpuInfoProvider.getCurrentInfo(res).freeDeviceMemoryInBytes();
    }

    @Override
    public void reserveMemory(long memoryInBytes) {
        // No-op
    }

    @Override
    public void releaseMemory(long memoryInBytes) {
        // No-op
    }
}
