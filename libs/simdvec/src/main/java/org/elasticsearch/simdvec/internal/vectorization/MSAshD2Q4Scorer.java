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
import java.lang.foreign.MemorySegment;

/**
 * Panama-accelerated scorer for D2Q4: 2-bit document codes scored against a
 * 4-bit quantized query via AND+popcount across 4 query bit-planes and 2 document
 * bit-planes.
 * <p>
 * The document vector has 2 bit-planes (low and high), each of {@code planeBytes} bytes,
 * laid out contiguously: {@code [plane0][plane1]}. The query has 4 bit-planes striped
 * in {@code queryQuantized}: {@code [qp0|qp1|qp2|qp3]} each of {@code planeBytes} bytes.
 * <p>
 * The raw dot product is computed as:
 * <pre>
 *   rawDot = fourStripeDot(query, docPlane0) + fourStripeDot(query, docPlane1) &lt;&lt; 1
 * </pre>
 * where {@code fourStripeDot} computes {@code sum_qp (1 << qp) * andBitCount(queryPlane[qp], docPlane)}.
 * <p>
 * Produces raw integer dot products without applying per-vector corrections.
 */
final class MSAshD2Q4Scorer extends MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorerBase
    implements
        MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorer<byte[]> {

    MSAshD2Q4Scorer(IndexInput in, int nDims, int planeBytes, int packedCodeBytes) {
        super(in, nDims, planeBytes, packedCodeBytes);
    }

    @Override
    public float score(byte[] queryQuantized) throws IOException {
        if (planeBytes >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return IndexInputUtils.withSlice(in, packedCodeBytes, scratch, seg -> (float) scoreTwoPlanes256(queryQuantized, seg));
            } else {
                return IndexInputUtils.withSlice(in, packedCodeBytes, scratch, seg -> (float) scoreTwoPlanes128(queryQuantized, seg));
            }
        }
        return Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean scoreBulk(byte[] queryQuantized, float[] scores, int blockSize) throws IOException {
        if (planeBytes >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            long totalBytes = (long) packedCodeBytes * blockSize;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        MemorySegment docSeg = seg.asSlice((long) j * packedCodeBytes, packedCodeBytes);
                        scores[j] = scoreTwoPlanes256(queryQuantized, docSeg);
                    }
                    return null;
                });
            } else {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        MemorySegment docSeg = seg.asSlice((long) j * packedCodeBytes, packedCodeBytes);
                        scores[j] = scoreTwoPlanes128(queryQuantized, docSeg);
                    }
                    return null;
                });
            }
            return true;
        }
        return false;
    }

    private int scoreTwoPlanes256(byte[] queryQuantized, MemorySegment docSeg) {
        long plane0Score = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct256(
            queryQuantized,
            docSeg,
            0L,
            planeBytes
        );
        long plane1Score = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct256(
            queryQuantized,
            docSeg,
            planeBytes,
            planeBytes
        );
        return (int) (plane0Score + (plane1Score << 1));
    }

    private int scoreTwoPlanes128(byte[] queryQuantized, MemorySegment docSeg) {
        long plane0Score = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct128(
            queryQuantized,
            docSeg,
            0L,
            planeBytes
        );
        long plane1Score = MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer.fourStripeBitDotProduct128(
            queryQuantized,
            docSeg,
            planeBytes,
            planeBytes
        );
        return (int) (plane0Score + (plane1Score << 1));
    }
}
