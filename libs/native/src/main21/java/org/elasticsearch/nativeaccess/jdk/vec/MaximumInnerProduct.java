/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jdk.vec;

import java.lang.foreign.MemorySegment;

// Scalar Quantized vectors are inherently bytes.
final class MaximumInnerProduct extends AbstractScalarQuantizedVectorScorer {

    MaximumInnerProduct(int dims, int maxOrd, float scoreCorrectionConstant, VectorDataInput data) {
        super(dims, maxOrd, scoreCorrectionConstant, data);
    }

    @Override
    public float score(int firstOrd, int secondOrd) {
        final int length = dims;
        int firstByteOffset = firstOrd * (length + Float.BYTES);
        MemorySegment firstSeg = data.addressFor(firstByteOffset, length);
        float firstOffset = data.readFloat(firstByteOffset + length);

        int secondByteOffset = secondOrd * (length + Float.BYTES);
        MemorySegment secondSeg = data.addressFor(secondByteOffset, length);
        float secondOffset = data.readFloat(secondByteOffset + length);

        int dotProduct = NativeVectorDistance.dotProduct(firstSeg, secondSeg, length);
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
