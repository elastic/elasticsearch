/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ESNextOSQVectorsScorer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;
import org.elasticsearch.simdvec.internal.MemorySegmentES92Int7VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

final class PanamaESVectorizationProvider extends ESVectorizationProvider {

    private final ESVectorUtilSupport vectorUtilSupport;

    PanamaESVectorizationProvider() {
        vectorUtilSupport = new PanamaESVectorUtilSupport();
    }

    @Override
    public ESVectorUtilSupport getVectorUtilSupport() {
        return vectorUtilSupport;
    }

    @Override
    public ESNextOSQVectorsScorer newESNextOSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize
    ) throws IOException {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
        if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS
            && unwrappedInput instanceof MemorySegmentAccessInput msai
            && queryBits == 4
            && (indexBits == 1 || indexBits == 2 || indexBits == 4)) {
            MemorySegment ms = msai.segmentSliceOrNull(0, unwrappedInput.length());
            if (ms != null) {
                return new MemorySegmentESNextOSQVectorsScorer(unwrappedInput, queryBits, indexBits, dimension, dataLength, bulkSize, ms);
            }
        }
        return new ESNextOSQVectorsScorer(input, queryBits, indexBits, dimension, dataLength, bulkSize);
    }

    @Override
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
        if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS && unwrappedInput instanceof MemorySegmentAccessInput msai) {
            MemorySegment ms = msai.segmentSliceOrNull(0, unwrappedInput.length());
            if (ms != null) {
                return new MemorySegmentES91OSQVectorsScorer(unwrappedInput, dimension, bulkSize, ms);
            }
        }
        return new OnHeapES91OSQVectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES92Int7VectorsScorer newES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
        if (unwrappedInput instanceof MemorySegmentAccessInput msai) {
            MemorySegment ms = msai.segmentSliceOrNull(0, unwrappedInput.length());
            if (ms != null) {
                return new MemorySegmentES92Int7VectorsScorer(unwrappedInput, dimension, bulkSize, ms);
            }
        }
        return new ES92Int7VectorsScorer(input, dimension, bulkSize);
    }
}
