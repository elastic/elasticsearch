/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

// Scalar Quantized vectors are inherently bytes.
public final class Int7Euclidean extends AbstractInt7ScalarQuantizedVectorScorer {

    public Int7Euclidean(int dims, int maxOrd, float scoreCorrectionConstant, IndexInput input) {
        super(
            dims,
            maxOrd,
            scoreCorrectionConstant,
            input,
            ScalarQuantizedVectorSimilarity.fromVectorSimilarity(VectorSimilarityFunction.EUCLIDEAN, scoreCorrectionConstant)
        );
    }

    @Override
    public float score(int firstOrd, int secondOrd) throws IOException {
        checkOrdinal(firstOrd);
        checkOrdinal(secondOrd);

        final int length = dims;
        int firstByteOffset = firstOrd * (length + Float.BYTES);
        int secondByteOffset = secondOrd * (length + Float.BYTES);

        MemorySegment firstSeg = segmentSlice(firstByteOffset, length);
        MemorySegment secondSeg = segmentSlice(secondByteOffset, length);

        if (firstSeg != null && secondSeg != null) {
            int squareDistance = squareDistance7u(firstSeg, secondSeg, length);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        } else {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
    }
}
