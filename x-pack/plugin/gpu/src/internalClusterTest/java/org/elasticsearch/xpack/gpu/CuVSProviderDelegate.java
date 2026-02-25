/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import com.nvidia.cuvs.BruteForceIndex;
import com.nvidia.cuvs.CagraIndex;
import com.nvidia.cuvs.CuVSDeviceMatrix;
import com.nvidia.cuvs.CuVSHostMatrix;
import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.CuVSResources;
import com.nvidia.cuvs.GPUInfoProvider;
import com.nvidia.cuvs.HnswIndex;
import com.nvidia.cuvs.TieredIndex;
import com.nvidia.cuvs.spi.CuVSProvider;

import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

class CuVSProviderDelegate implements CuVSProvider {
    private final CuVSProvider delegate;

    CuVSProviderDelegate(CuVSProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public CuVSResources newCuVSResources(Path path) throws Throwable {
        return delegate.newCuVSResources(path);
    }

    @Override
    public CuVSMatrix.Builder<CuVSHostMatrix> newHostMatrixBuilder(long l, long l1, CuVSMatrix.DataType dataType) {
        return delegate.newHostMatrixBuilder(l, l1, dataType);
    }

    @Override
    public CuVSMatrix.Builder<CuVSHostMatrix> newHostMatrixBuilder(
        long size,
        long columns,
        int rowStride,
        int columnStride,
        CuVSMatrix.DataType dataType
    ) {
        return delegate.newHostMatrixBuilder(size, columns, rowStride, columnStride, dataType);
    }

    @Override
    public CuVSMatrix.Builder<CuVSDeviceMatrix> newDeviceMatrixBuilder(
        CuVSResources cuVSResources,
        long l,
        long l1,
        CuVSMatrix.DataType dataType
    ) {
        return delegate.newDeviceMatrixBuilder(cuVSResources, l, l1, dataType);
    }

    @Override
    public CuVSMatrix.Builder<CuVSDeviceMatrix> newDeviceMatrixBuilder(
        CuVSResources cuVSResources,
        long l,
        long l1,
        int i,
        int i1,
        CuVSMatrix.DataType dataType
    ) {
        return delegate.newDeviceMatrixBuilder(cuVSResources, l, l1, i, i1, dataType);
    }

    @Override
    public MethodHandle newNativeMatrixBuilder() {
        return delegate.newNativeMatrixBuilder();
    }

    @Override
    public MethodHandle newNativeMatrixBuilderWithStrides() {
        return delegate.newNativeMatrixBuilderWithStrides();
    }

    @Override
    public CuVSMatrix newMatrixFromArray(float[][] floats) {
        return delegate.newMatrixFromArray(floats);
    }

    @Override
    public CuVSMatrix newMatrixFromArray(int[][] ints) {
        return delegate.newMatrixFromArray(ints);
    }

    @Override
    public CuVSMatrix newMatrixFromArray(byte[][] bytes) {
        return delegate.newMatrixFromArray(bytes);
    }

    @Override
    public BruteForceIndex.Builder newBruteForceIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newBruteForceIndexBuilder(cuVSResources);
    }

    @Override
    public CagraIndex.Builder newCagraIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newCagraIndexBuilder(cuVSResources);
    }

    @Override
    public HnswIndex.Builder newHnswIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newHnswIndexBuilder(cuVSResources);
    }

    @Override
    public TieredIndex.Builder newTieredIndexBuilder(CuVSResources cuVSResources) throws UnsupportedOperationException {
        return delegate.newTieredIndexBuilder(cuVSResources);
    }

    @Override
    public CagraIndex mergeCagraIndexes(CagraIndex[] cagraIndices) throws Throwable {
        return delegate.mergeCagraIndexes(cagraIndices);
    }

    @Override
    public GPUInfoProvider gpuInfoProvider() {
        return delegate.gpuInfoProvider();
    }
}
