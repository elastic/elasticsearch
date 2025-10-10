/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSMatrix;

import java.util.function.Supplier;

/** CuVSResourceManager that always throws. Useful to CPU-only testing. */
public class ThrowingCuVSResourceManager implements CuVSResourceManager {

    public static Supplier<CuVSResourceManager> supplier = ThrowingCuVSResourceManager::new;

    @Override
    public ManagedCuVSResources acquire(int numVectors, int dims, CuVSMatrix.DataType dataType) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public void finishedComputation(ManagedCuVSResources resources) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public void release(ManagedCuVSResources resources) {
        throw new AssertionError("should not reach here");
    }

    @Override
    public void shutdown() {
        throw new AssertionError("should not reach here");
    }
}
