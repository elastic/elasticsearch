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

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

class DatasetUtilsImpl implements DatasetUtils {

    private static final DatasetUtils INSTANCE = new DatasetUtilsImpl();

    private static final MethodHandle createDataset$mh = CuVSProvider.provider().newNativeMatrixBuilder();

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

    private DatasetUtilsImpl() {}

    @Override
    public CuVSMatrix fromInput(MemorySegment input, int numVectors, int dims, CuVSMatrix.DataType dataType) {
        if (input == null) {
            throw new IllegalArgumentException("input cannot be null");
        }
        if (numVectors < 0 || dims < 0) {
            throwIllegalArgumentException(numVectors, dims);
        }
        final int byteSize = dataType == CuVSMatrix.DataType.FLOAT ? Float.BYTES : Byte.BYTES;
        if (((long) numVectors * dims * byteSize) > input.byteSize()) {
            throwIllegalArgumentException(input, numVectors, dims);
        }
        return fromMemorySegment(input, numVectors, dims, dataType);
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
