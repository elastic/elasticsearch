/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.vec.internal.IndexInputUtils;
import org.elasticsearch.vec.internal.Int7SQVectorScorerSupplier.DotProductSupplier;
import org.elasticsearch.vec.internal.Int7SQVectorScorerSupplier.EuclideanSupplier;
import org.elasticsearch.vec.internal.Int7SQVectorScorerSupplier.MaxInnerProductSupplier;

import java.util.Optional;

class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE;

    private VectorScorerFactoryImpl() {}

    static {
        INSTANCE = NativeAccess.instance().getVectorSimilarityFunctions().map(ignore -> new VectorScorerFactoryImpl()).orElse(null);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7ScalarQuantizedVectorScorer(
        VectorSimilarityType similarityType,
        IndexInput input,
        RandomAccessQuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        input = IndexInputUtils.unwrapAndCheckInputOrNull(input);
        if (input == null) {
            return Optional.empty(); // the input type is not MemorySegment based
        }
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new DotProductSupplier(input, values, scoreCorrectionConstant));
            case EUCLIDEAN -> Optional.of(new EuclideanSupplier(input, values, scoreCorrectionConstant));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(input, values, scoreCorrectionConstant));
        };
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }
}
