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
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;
import org.elasticsearch.simdvec.ES93BinaryQuantizedVectorScorer;
import org.elasticsearch.simdvec.ES940OSQVectorsScorer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;
import org.elasticsearch.simdvec.internal.IndexInputUtils;
import org.elasticsearch.simdvec.internal.MemorySegmentES92Int7VectorsScorer;

import java.io.IOException;

final class PanamaESVectorizationProvider extends ESVectorizationProvider {

    private final ESVectorUtilSupport vectorUtilSupport;

    private static final boolean NATIVE_SUPPORTED = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    PanamaESVectorizationProvider() {
        vectorUtilSupport = new PanamaESVectorUtilSupport();
    }

    @Override
    public ESVectorUtilSupport getVectorUtilSupport() {
        return vectorUtilSupport;
    }

    @Override
    public ES940OSQVectorsScorer newES940OSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize,
        ES940OSQVectorsScorer.SymmetricInt4Encoding int4Encoding
    ) {
        if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS
            && dataLength >= 16
            && ((queryBits == 4 && (indexBits == 1 || indexBits == 2 || indexBits == 4)) || (queryBits == 7 && indexBits == 7))) {
            IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
            unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
            if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
                return new MemorySegmentES940OSQVectorsScorer(
                    unwrappedInput,
                    queryBits,
                    indexBits,
                    dimension,
                    dataLength,
                    bulkSize,
                    int4Encoding
                );
            }
        }
        return new ES940OSQVectorsScorer(input, queryBits, indexBits, dimension, dataLength, bulkSize, int4Encoding);
    }

    @Override
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
            unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
            if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
                return new MemorySegmentES91OSQVectorsScorer(unwrappedInput, dimension, bulkSize);
            }
        }
        return new OnHeapES91OSQVectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES92Int7VectorsScorer newES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) {
        IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
        unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);

        if (IndexInputUtils.canUseSegmentSlices(unwrappedInput)) {
            return new MemorySegmentES92Int7VectorsScorer(unwrappedInput, dimension, bulkSize);
        }
        return new ES92Int7VectorsScorer(input, dimension, bulkSize);
    }

    @Override
    public ES93BinaryQuantizedVectorScorer newES93BinaryQuantizedVectorScorer(IndexInput input, int dimensions, int vectorLengthInBytes)
        throws IOException {
        if (NATIVE_SUPPORTED && JdkFeatures.SUPPORTS_HEAP_SEGMENTS) {
            IndexInput unwrappedInput = FilterIndexInput.unwrapOnlyTest(input);
            unwrappedInput = MemorySegmentAccessInputAccess.unwrap(unwrappedInput);
            return new NativeBinaryQuantizedVectorScorer(unwrappedInput, dimensions, vectorLengthInBytes);
        }
        return new DefaultES93BinaryQuantizedVectorScorer(input, dimensions, vectorLengthInBytes);
    }
}
