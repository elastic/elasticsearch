/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSResources;

/**
 * Abstracts GPU memory tracking (total vs available)
 */
interface GPUMemoryService {

    long totalMemoryInBytes(CuVSResources res);

    long availableMemoryInBytes(CuVSResources res);

    void reserveMemory(long memoryInBytes);

    void releaseMemory(long memoryInBytes);
}
