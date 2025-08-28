/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import com.nvidia.cuvs.CuVSMatrix;

import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.IOException;

public interface DatasetUtils {

    static DatasetUtils getInstance() {
        return DatasetUtilsImpl.getInstance();
    }

    /** Returns a Dataset over the float32 vectors in the input. */
    CuVSMatrix fromInput(MemorySegmentAccessInput input, int numVectors, int dims, CuVSMatrix.DataType dataType) throws IOException;
}
