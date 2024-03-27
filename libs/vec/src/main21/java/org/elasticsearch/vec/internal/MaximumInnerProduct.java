/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.elasticsearch.vec.internal.IndexInputUtils.segmentSlice;

// Scalar Quantized vectors are inherently bytes.
public final class MaximumInnerProduct extends AbstractScalarQuantizedVectorScorer {

    public MaximumInnerProduct(int dims, int maxOrd, float scoreCorrectionConstant, IndexInput input) {
        super(dims, maxOrd, scoreCorrectionConstant, input);
    }

    @Override
    public float score(int firstOrd, int secondOrd) throws IOException {
        checkOrdinal(firstOrd);
        checkOrdinal(secondOrd);

        final int length = dims;
        int firstByteOffset = firstOrd * (length + Float.BYTES);
        MemorySegment firstSeg = segmentSlice(input, firstByteOffset, length);
        input.seek(firstByteOffset + length);
        float firstOffset = Float.intBitsToFloat(input.readInt());

        int secondByteOffset = secondOrd * (length + Float.BYTES);
        MemorySegment secondSeg = segmentSlice(input, secondByteOffset, length);
        input.seek(secondByteOffset + length);
        float secondOffset = Float.intBitsToFloat(input.readInt());

        int dotProduct = dotProduct(firstSeg, secondSeg, length);
        float adjustedDistance = dotProduct * scoreCorrectionConstant + firstOffset + secondOffset;
        return scaleMaxInnerProductScore(adjustedDistance);
    }

    /**
     * Returns a scaled score preventing negative scores for maximum-inner-product
     * @param rawSimilarity the raw similarity between two vectors
     */
    static float scaleMaxInnerProductScore(float rawSimilarity) {
        if (rawSimilarity < 0) {
            return 1 / (1 + -1 * rawSimilarity);
        }
        return rawSimilarity + 1;
    }
}
