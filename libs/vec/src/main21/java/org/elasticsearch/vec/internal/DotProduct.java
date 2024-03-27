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

// Scalar Quantized vectors are inherently byte sized, so dims is equal to the length in bytes.
public final class DotProduct extends AbstractScalarQuantizedVectorScorer {

    public DotProduct(int dims, int maxOrd, float scoreCorrectionConstant, IndexInput input) {
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
        return (1 + adjustedDistance) / 2;
    }
}
