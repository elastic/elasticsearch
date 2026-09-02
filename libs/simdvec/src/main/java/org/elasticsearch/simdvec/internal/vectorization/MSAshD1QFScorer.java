/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Panama-accelerated scorer for D1QF: 1-bit document codes scored against a
 * float-precision projected query via masked float addition (ipFloatBit on MemorySegment).
 * <p>
 * Produces raw dot products (weighted sum over bit planes minus centering offset)
 * without applying per-vector corrections.
 */
final class MSAshD1QFScorer extends MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorerBase
    implements
        MemorySegmentESNextAshVectorsScorer.AshMemorySegmentScorer<float[]> {

    private static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;
    private static final VectorSpecies<Float> FLOAT_SPECIES_128 = FloatVector.SPECIES_128;

    private static final ValueLayout.OfInt LAYOUT_BE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfShort LAYOUT_BE_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    MSAshD1QFScorer(IndexInput in, int nDims, int planeBytes, int packedCodeBytes) {
        super(in, nDims, planeBytes, packedCodeBytes);
    }

    @Override
    public float score(float[] queryTransformed) throws IOException {
        if (planeBytes >= 2 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            float querySum = ESVectorUtil.sum(queryTransformed, nDims);
            float centerOffset = 0.5f; // (2-1)/2 for 1-bit
            float rawDot;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                rawDot = IndexInputUtils.withSlice(in, planeBytes, scratch, seg -> ipFloatBitSegment256(queryTransformed, seg, 0, nDims));
            } else {
                rawDot = IndexInputUtils.withSlice(in, planeBytes, scratch, seg -> ipFloatBitSegment128(queryTransformed, seg, 0, nDims));
            }
            return rawDot - centerOffset * querySum;
        }
        return Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean scoreBulk(float[] queryTransformed, float[] scores, int blockSize) throws IOException {
        if (planeBytes >= 2 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            float querySum = ESVectorUtil.sum(queryTransformed, nDims);
            float centerOffset = 0.5f;
            long totalBytes = (long) planeBytes * blockSize;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        long offset = (long) j * planeBytes;
                        float ipfb = ipFloatBitSegment256(queryTransformed, seg, offset, nDims);
                        scores[j] = ipfb - centerOffset * querySum;
                    }
                    return null;
                });
            } else {
                IndexInputUtils.withSlice(in, totalBytes, scratch, seg -> {
                    for (int j = 0; j < blockSize; j++) {
                        long offset = (long) j * planeBytes;
                        float ipfb = ipFloatBitSegment128(queryTransformed, seg, offset, nDims);
                        scores[j] = ipfb - centerOffset * querySum;
                    }
                    return null;
                });
            }
            return true;
        }
        return false;
    }

    /**
     * Computes ipFloatBit: sum of q[i] where bit i is set in the MemorySegment.
     * Bits are packed MSB-first within each byte. 256-bit SIMD path.
     */
    static float ipFloatBitSegment256(float[] q, MemorySegment d, long baseOffset, int qLength) {
        int i = 0;
        FloatVector acc = FloatVector.zero(FLOAT_SPECIES_256);

        int sectionLength = FLOAT_SPECIES_256.length() * 4;
        if (qLength >= sectionLength) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES_256);
            int limit = (qLength / sectionLength) * sectionLength;
            for (; i < limit; i += sectionLength) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length());
                var floats2 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length() * 2);
                var floats3 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length() * 3);

                int bits = d.get(LAYOUT_BE_INT, baseOffset + i / 8);
                long maskBits = Integer.reverse(bits);
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 8);
                var mask2 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 16);
                var mask3 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 24);

                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
                acc2 = acc2.add(floats2, mask2);
                acc3 = acc3.add(floats3, mask3);
            }
            acc = acc0.add(acc1).add(acc2).add(acc3);
        }

        // 8-float tail (1 byte of bits)
        sectionLength = FLOAT_SPECIES_256.length();
        if (qLength - i >= sectionLength) {
            int limit = i + ((qLength - i) / sectionLength) * sectionLength;
            for (; i < limit; i += sectionLength) {
                var floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);
                long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
                var mask = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);
                acc = acc.add(floats, mask);
            }
        }

        // any remaining
        if (i < qLength) {
            var loadMask = FLOAT_SPECIES_256.indexInRange(i, qLength);
            var floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i, loadMask);
            long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
            var addMask = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);
            acc = acc.add(floats, addMask);
        }

        return acc.reduceLanes(VectorOperators.ADD);
    }

    /**
     * Computes ipFloatBit on a MemorySegment. 128-bit SIMD path.
     */
    static float ipFloatBitSegment128(float[] q, MemorySegment d, long baseOffset, int qLength) {
        int i = 0;
        FloatVector acc = FloatVector.zero(FLOAT_SPECIES_128);

        int sectionLength = FLOAT_SPECIES_128.length() * 4;
        if (qLength >= sectionLength) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_128);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_128);
            FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES_128);
            FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES_128);
            int limit = (qLength / sectionLength) * sectionLength;
            for (; i < limit; i += sectionLength) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i + FLOAT_SPECIES_128.length());
                var floats2 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i + FLOAT_SPECIES_128.length() * 2);
                var floats3 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i + FLOAT_SPECIES_128.length() * 3);

                long maskBits = Integer.reverse(d.get(LAYOUT_BE_SHORT, baseOffset + i / 8)) >> 16;
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits >> 4);
                var mask2 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits >> 8);
                var mask3 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits >> 12);

                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
                acc2 = acc2.add(floats2, mask2);
                acc3 = acc3.add(floats3, mask3);
            }
            acc = acc0.add(acc1).add(acc2).add(acc3);
        }

        // 8-float tail (1 byte of bits)
        sectionLength = FLOAT_SPECIES_128.length() * 2;
        if (qLength - i >= sectionLength) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_128);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_128);
            int limit = i + ((qLength - i) / sectionLength) * sectionLength;
            for (; i < limit; i += sectionLength) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i + FLOAT_SPECIES_128.length());
                long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits >> 4);
                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
            }
            acc = acc.add(acc0).add(acc1);
        }

        // any remaining (less than 8 floats)
        if (i < qLength) {
            long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
            for (; i < qLength; i += FLOAT_SPECIES_128.length()) {
                var mask = FLOAT_SPECIES_128.indexInRange(i, qLength);
                var floats = FloatVector.fromArray(FLOAT_SPECIES_128, q, i, mask);
                var addMask = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits);
                acc = acc.add(floats, addMask);
                maskBits >>= 4;
            }
        }

        return acc.reduceLanes(VectorOperators.ADD);
    }
}
