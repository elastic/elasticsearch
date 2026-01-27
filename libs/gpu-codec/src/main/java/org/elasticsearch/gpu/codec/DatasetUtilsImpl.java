/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import com.nvidia.cuvs.CuVSMatrix;
import com.nvidia.cuvs.spi.CuVSProvider;

import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

class DatasetUtilsImpl implements DatasetUtils {

    private static final DatasetUtils INSTANCE = new DatasetUtilsImpl();

    private static final MethodHandle createDataset$mh = CuVSProvider.provider().newNativeMatrixBuilder();
    private static final MethodHandle createDatasetWithStrides$mh = CuVSProvider.provider().newNativeMatrixBuilderWithStrides();

    static DatasetUtils getInstance() {
        return INSTANCE;
    }

    static CuVSMatrix fromMemorySegment(MemorySegment memorySegment, int size, int dimensions, CuVSMatrix.DataType dataType) {
        try {
            return (CuVSMatrix) createDataset$mh.invokeExact(memorySegment, size, dimensions, dataType);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static CuVSMatrix fromMemorySegment(
        MemorySegment memorySegment,
        int size,
        int dimensions,
        int rowStride,
        int columnStride,
        CuVSMatrix.DataType dataType
    ) {
        try {
            return (CuVSMatrix) createDatasetWithStrides$mh.invokeExact(memorySegment, size, dimensions, rowStride, columnStride, dataType);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private DatasetUtilsImpl() {}

    @Override
    public CuVSMatrix fromInput(MemorySegmentAccessInput input, int numVectors, int dims, CuVSMatrix.DataType dataType) throws IOException {
        if (numVectors < 0 || dims < 0) {
            throwIllegalArgumentException(numVectors, dims);
        }
        return createCuVSMatrix(input, 0L, input.length(), numVectors, dims, dataType);
    }

    @Override
    public CuVSMatrix fromInput(
        MemorySegmentAccessInput input,
        int numVectors,
        int dims,
        int rowStride,
        int columnStride,
        CuVSMatrix.DataType dataType
    ) throws IOException {
        if (numVectors < 0 || dims < 0) {
            throwIllegalArgumentException(numVectors, dims);
        }
        return createCuVSMatrix(input, 0L, input.length(), numVectors, dims, rowStride, columnStride, dataType);
    }

    @Override
    public CuVSMatrix fromSlice(MemorySegmentAccessInput input, long pos, long len, int numVectors, int dims, CuVSMatrix.DataType dataType)
        throws IOException {
        if (pos < 0 || len < 0) {
            throw new IllegalArgumentException("pos and len must be positive");
        }
        return createCuVSMatrix(input, pos, len, numVectors, dims, dataType);
    }

    private static CuVSMatrix createCuVSMatrix(
        MemorySegmentAccessInput input,
        long pos,
        long len,
        int numVectors,
        int dims,
        CuVSMatrix.DataType dataType
    ) throws IOException {
        MemorySegment ms = input.segmentSliceOrNull(pos, len);
        assert ms != null; // TODO: this can be null if larger than 16GB or ...
        final int byteSize = dataType == CuVSMatrix.DataType.FLOAT ? Float.BYTES : Byte.BYTES;
        if (((long) numVectors * dims * byteSize) > ms.byteSize()) {
            throwIllegalArgumentException(ms, numVectors, dims);
        }
        return fromMemorySegment(ms, numVectors, dims, dataType);
    }

    private static CuVSMatrix createCuVSMatrix(
        MemorySegmentAccessInput input,
        long pos,
        long len,
        int numVectors,
        int dims,
        int rowStride,
        int columnStride,
        CuVSMatrix.DataType dataType
    ) throws IOException {
        MemorySegment ms = input.segmentSliceOrNull(pos, len);
        assert ms != null;
        final int byteSize = dataType == CuVSMatrix.DataType.FLOAT ? Float.BYTES : Byte.BYTES;
        if (((long) numVectors * rowStride * byteSize) > ms.byteSize()) {
            throwIllegalArgumentException(ms, numVectors, dims);
        }
        return fromMemorySegment(ms, numVectors, dims, rowStride, columnStride, dataType);
    }

    static void throwIllegalArgumentException(MemorySegment ms, int numVectors, int dims) {
        var s = "segment of size [" + ms.byteSize() + "] too small for expected " + numVectors + " float vectors of " + dims + " dims";
        throw new IllegalArgumentException(s);
    }

    static void throwIllegalArgumentException(int numVectors, int dims) {
        String s;
        if (numVectors < 0) {
            s = "negative number of vectors: " + numVectors;
        } else {
            s = "negative vector dims: " + dims;
        }
        throw new IllegalArgumentException(s);
    }
}
