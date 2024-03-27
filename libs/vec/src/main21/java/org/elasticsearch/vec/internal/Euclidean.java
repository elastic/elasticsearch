/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.IndexInput;

import java.lang.foreign.MemorySegment;

import static org.elasticsearch.vec.internal.IndexInputUtils.segmentSlice;

// Scalar Quantized vectors are inherently bytes.
public final class Euclidean extends AbstractScalarQuantizedVectorScorer {

    public Euclidean(int dims, int maxOrd, float scoreCorrectionConstant, IndexInput input) {
        super(dims, maxOrd, scoreCorrectionConstant, input);
    }

    @Override
    public float score(int firstOrd, int secondOrd) {
        checkOrdinal(firstOrd);
        checkOrdinal(secondOrd);

        final int length = dims;
        int firstByteOffset = firstOrd * (length + Float.BYTES);
        MemorySegment firstSeg = segmentSlice(input, firstByteOffset, length);

        int secondByteOffset = secondOrd * (length + Float.BYTES);
        MemorySegment secondSeg = segmentSlice(input, secondByteOffset, length);

        int squareDistance = squareDistance(firstSeg, secondSeg, length);
        float adjustedDistance = squareDistance * scoreCorrectionConstant;
        return 1 / (1f + adjustedDistance);
    }
}
