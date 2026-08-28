/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.lucene.store.IndexInputUtils;

import java.io.IOException;

/**
 * Panama-accelerated scorer for D1Q4: 1-bit document codes scored against a
 * 4-bit quantized query via AND+popcount across 4 query bit-planes and 1 document
 * bit-plane.
 * <p>
 * Reuses the {@code fourStripeBitDotProduct} methods from
 * {@link MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer} which compute exactly
 * this: a 4-stripe AND+popcount of a {@code byte[]} query against a
 * {@link java.lang.foreign.MemorySegment} document vector.
 * <p>
 * Produces raw integer dot products without applying per-vector corrections.
 */
final class MSAshD1Q4Scorer extends MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorerBase
    implements
        MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorer<byte[]> {

    MSAshD1Q4Scorer(IndexInput in, int nDims, int planeBytes, int packedCodeBytes) {
        super(in, nDims, planeBytes, packedCodeBytes);
    }

    @Override
    public float score(byte[] queryQuantized) throws IOException {
        if (planeBytes >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return IndexInputUtils.withSlice(
                    in,
                    planeBytes,
                    scratch,
                    seg -> (float) MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct256(
                        queryQuantized,
                        seg,
                        0L,
                        planeBytes
                    )
                );
            } else {
                return IndexInputUtils.withSlice(
                    in,
                    planeBytes,
                    scratch,
                    seg -> (float) MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct128(
                        queryQuantized,
                        seg,
                        0L,
                        planeBytes
                    )
                );
            }
        }
        return Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean scoreBulk(byte[] queryQuantized, float[] scores, int blockSize) throws IOException {
        if (planeBytes >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            long totalBytes = (long) planeBytes * blockSize;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        long offset = (long) j * planeBytes;
                        scores[j] = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct256(
                            queryQuantized,
                            seg,
                            offset,
                            planeBytes
                        );
                    }
                    return null;
                });
            } else {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        long offset = (long) j * planeBytes;
                        scores[j] = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct128(
                            queryQuantized,
                            seg,
                            offset,
                            planeBytes
                        );
                    }
                    return null;
                });
            }
            return true;
        }
        return false;
    }
}
