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
        float sum = 0;

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
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        int sectionLength2 = FLOAT_SPECIES_256.length();
        if (qLength - i >= sectionLength2) {
            FloatVector acc = FloatVector.zero(FLOAT_SPECIES_256);
            int limit = i + ((qLength - i) / sectionLength2) * sectionLength2;
            for (; i < limit; i += sectionLength2) {
                var floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);
                long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
                var mask = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);
                acc = acc.add(floats, mask);
            }
            sum += acc.reduceLanes(VectorOperators.ADD);
        }

        if (i < qLength) {
            sum += ipFloatBitScalarTail(q, i, d, baseOffset + i / 8, qLength - i);
        }
        return sum;
    }

    /**
     * Computes ipFloatBit on a MemorySegment. 128-bit SIMD path.
     */
    static float ipFloatBitSegment128(float[] q, MemorySegment d, long baseOffset, int qLength) {
        int i = 0;
        float sum = 0;

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
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        int sectionLength2 = FLOAT_SPECIES_128.length() * 2;
        if (qLength - i >= sectionLength2) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_128);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_128);
            int limit = i + ((qLength - i) / sectionLength2) * sectionLength2;
            for (; i < limit; i += sectionLength2) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_128, q, i + FLOAT_SPECIES_128.length());
                long maskBits = Integer.reverse(d.get(ValueLayout.JAVA_BYTE, baseOffset + i / 8)) >> 24;
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_128, maskBits >> 4);
                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
            }
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD);
        }

        if (i < qLength) {
            sum += ipFloatBitScalarTail(q, i, d, baseOffset + i / 8, qLength - i);
        }
        return sum;
    }

    /** Scalar tail for ipFloatBit from MemorySegment. */
    private static float ipFloatBitScalarTail(float[] q, int qOffset, MemorySegment d, long dOffset, int length) {
        float acc = 0;
        int byteCount = length >>> 3;
        for (int i = 0; i < byteCount; i++) {
            byte mask = d.get(ValueLayout.JAVA_BYTE, dOffset + i);
            int base = qOffset + i * Byte.SIZE;
            acc = Math.fma(q[base], (mask >> 7) & 1, acc);
            acc = Math.fma(q[base + 1], (mask >> 6) & 1, acc);
            acc = Math.fma(q[base + 2], (mask >> 5) & 1, acc);
            acc = Math.fma(q[base + 3], (mask >> 4) & 1, acc);
            acc = Math.fma(q[base + 4], (mask >> 3) & 1, acc);
            acc = Math.fma(q[base + 5], (mask >> 2) & 1, acc);
            acc = Math.fma(q[base + 6], (mask >> 1) & 1, acc);
            acc = Math.fma(q[base + 7], mask & 1, acc);
        }
        int tail = length & 7;
        if (tail > 0) {
            byte mask = d.get(ValueLayout.JAVA_BYTE, dOffset + byteCount);
            int base = qOffset + byteCount * Byte.SIZE;
            for (int j = 0; j < tail; j++) {
                acc = Math.fma(q[base + j], (mask >> (7 - j)) & 1, acc);
            }
        }
        return acc;
    }
}
