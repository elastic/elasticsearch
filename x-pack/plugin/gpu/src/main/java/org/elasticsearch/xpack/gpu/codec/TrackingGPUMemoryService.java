/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

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
