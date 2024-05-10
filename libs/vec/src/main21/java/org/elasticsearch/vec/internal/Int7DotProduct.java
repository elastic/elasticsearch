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

// Scalar Quantized vectors are inherently byte sized, so dims is equal to the length in bytes.
public final class Int7DotProduct extends AbstractInt7ScalarQuantizedVectorScorer {

    public Int7DotProduct(int dims, int maxOrd, float scoreCorrectionConstant, IndexInput input) {
        super(
            dims,
            maxOrd,
            scoreCorrectionConstant,
            input,
            ScalarQuantizedVectorSimilarity.fromVectorSimilarity(VectorSimilarityFunction.DOT_PRODUCT, scoreCorrectionConstant)
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
        input.seek(firstByteOffset + length);
        float firstOffset = Float.intBitsToFloat(input.readInt());

        MemorySegment secondSeg = segmentSlice(secondByteOffset, length);
        input.seek(secondByteOffset + length);
        float secondOffset = Float.intBitsToFloat(input.readInt());

        if (firstSeg != null && secondSeg != null) {
            int dotProduct = dotProduct7u(firstSeg, secondSeg, length);
            float adjustedDistance = dotProduct * scoreCorrectionConstant + firstOffset + secondOffset;
            return (1 + adjustedDistance) / 2;
        } else {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
    }
}
