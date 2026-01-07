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

import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.IOException;

interface DatasetUtils {

    static DatasetUtils getInstance() {
        return DatasetUtilsImpl.getInstance();
    }

    /** Returns a Dataset over the vectors of type {@code dataType} in the input. */
    CuVSMatrix fromInput(MemorySegmentAccessInput input, int numVectors, int dims, CuVSMatrix.DataType dataType) throws IOException;

    CuVSMatrix fromInput(
        MemorySegmentAccessInput input,
        int numVectors,
        int dims,
        int rowStride,
        int columnStride,
        CuVSMatrix.DataType dataType
    ) throws IOException;

    /** Returns a Dataset over an input slice */
    CuVSMatrix fromSlice(MemorySegmentAccessInput input, long pos, long len, int numVectors, int dims, CuVSMatrix.DataType dataType)
        throws IOException;
}
