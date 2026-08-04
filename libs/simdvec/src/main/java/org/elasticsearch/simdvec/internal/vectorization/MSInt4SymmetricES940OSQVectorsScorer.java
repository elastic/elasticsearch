/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.simdvec.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
final class MSInt4SymmetricES940OSQVectorsScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    MSInt4SymmetricES940OSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return quantizeScoreSymmetric256(q);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return quantizeScoreSymmetric128(q);
            }
        }
        return Long.MIN_VALUE;
    }

    private long quantizeScoreSymmetric128(byte[] q) throws IOException {
        int stripe0 = (int) quantizeScore128(q);
        int stripe1 = (int) quantizeScore128(q);
        int stripe2 = (int) quantizeScore128(q);
        int stripe3 = (int) quantizeScore128(q);
        return stripe0 + ((long) stripe1 << 1) + ((long) stripe2 << 2) + ((long) stripe3 << 3);
    }

    private long quantizeScoreSymmetric256(byte[] q) throws IOException {
        int stripe0 = (int) quantizeScore256(q);
        int stripe1 = (int) quantizeScore256(q);
        int stripe2 = (int) quantizeScore256(q);
        int stripe3 = (int) quantizeScore256(q);
        return stripe0 + ((long) stripe1 << 1) + ((long) stripe2 << 2) + ((long) stripe3 << 3);
    }

    private long quantizeScore256(byte[] q) throws IOException {
        int size = length / 4;
        return IndexInputUtils.withSlice(in, size, scratch::get, segment -> quantizeScore256Impl(q, segment, size));
    }

    private static long quantizeScore256Impl(byte[] q, MemorySegment memorySegment, int size) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        if (size >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
            int limit = ByteVector.SPECIES_256.loopBound(size);
            var sum0 = LongVector.zero(LONG_SPECIES_256);
            var sum1 = LongVector.zero(LONG_SPECIES_256);
            var sum2 = LongVector.zero(LONG_SPECIES_256);
            var sum3 = LongVector.zero(LONG_SPECIES_256);
            for (; i < limit; i += ByteVector.SPECIES_256.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 3).reinterpretAsLongs();
                var vd = LongVector.fromMemorySegment(LONG_SPECIES_256, memorySegment, i, ByteOrder.LITTLE_ENDIAN);
                sum0 = sum0.add(vq0.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum1 = sum1.add(vq1.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum2 = sum2.add(vq2.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum3 = sum3.add(vq3.and(vd).lanewise(VectorOperators.BIT_COUNT));
            }
            subRet0 += sum0.reduceLanes(VectorOperators.ADD);
            subRet1 += sum1.reduceLanes(VectorOperators.ADD);
            subRet2 += sum2.reduceLanes(VectorOperators.ADD);
            subRet3 += sum3.reduceLanes(VectorOperators.ADD);
        }

        if (size - i >= ByteVector.SPECIES_128.vectorByteSize()) {
            var sum0 = LongVector.zero(LONG_SPECIES_128);
            var sum1 = LongVector.zero(LONG_SPECIES_128);
            var sum2 = LongVector.zero(LONG_SPECIES_128);
            var sum3 = LongVector.zero(LONG_SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(size);
            for (; i < limit; i += ByteVector.SPECIES_128.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 3).reinterpretAsLongs();
                var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, memorySegment, i, ByteOrder.LITTLE_ENDIAN);
                sum0 = sum0.add(vq0.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum1 = sum1.add(vq1.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum2 = sum2.add(vq2.and(vd).lanewise(VectorOperators.BIT_COUNT));
                sum3 = sum3.add(vq3.and(vd).lanewise(VectorOperators.BIT_COUNT));
            }
            subRet0 += sum0.reduceLanes(VectorOperators.ADD);
            subRet1 += sum1.reduceLanes(VectorOperators.ADD);
            subRet2 += sum2.reduceLanes(VectorOperators.ADD);
            subRet3 += sum3.reduceLanes(VectorOperators.ADD);
        }
        // process scalar tail
        for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = memorySegment.get(LAYOUT_LE_LONG, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = memorySegment.get(LAYOUT_LE_INT, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
        }
        for (; i < size; i++) {
            int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + size] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * size] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * size] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    private long quantizeScore128(byte[] q) throws IOException {
        int size = length / 4;
        return IndexInputUtils.withSlice(in, size, scratch::get, segment -> quantizeScore128Impl(q, segment, size));
    }

    private static long quantizeScore128Impl(byte[] q, MemorySegment memorySegment, int size) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;

        var sum0 = IntVector.zero(INT_SPECIES_128);
        var sum1 = IntVector.zero(INT_SPECIES_128);
        var sum2 = IntVector.zero(INT_SPECIES_128);
        var sum3 = IntVector.zero(INT_SPECIES_128);
        int limit = ByteVector.SPECIES_128.loopBound(size);
        for (; i < limit; i += ByteVector.SPECIES_128.length()) {
            var vd = IntVector.fromMemorySegment(INT_SPECIES_128, memorySegment, i, ByteOrder.LITTLE_ENDIAN);
            var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
            var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size).reinterpretAsInts();
            var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 2).reinterpretAsInts();
            var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 3).reinterpretAsInts();
            sum0 = sum0.add(vd.and(vq0).lanewise(VectorOperators.BIT_COUNT));
            sum1 = sum1.add(vd.and(vq1).lanewise(VectorOperators.BIT_COUNT));
            sum2 = sum2.add(vd.and(vq2).lanewise(VectorOperators.BIT_COUNT));
            sum3 = sum3.add(vd.and(vq3).lanewise(VectorOperators.BIT_COUNT));
        }
        subRet0 += sum0.reduceLanes(VectorOperators.ADD);
        subRet1 += sum1.reduceLanes(VectorOperators.ADD);
        subRet2 += sum2.reduceLanes(VectorOperators.ADD);
        subRet3 += sum3.reduceLanes(VectorOperators.ADD);
        // process scalar tail
        for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = memorySegment.get(LAYOUT_LE_LONG, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = memorySegment.get(LAYOUT_LE_INT, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
        }
        for (; i < size; i++) {
            int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + size] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * size] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * size] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    @Override
    public boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, count, scores);
                return true;
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, count, scores);
                return true;
            }
        }
        return false;
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = quantizeScoreSymmetric128(q);
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = quantizeScoreSymmetric256(q);
        }
    }

    @Override
    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, bulkSize, scores);
                return applyCorrections256Bulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    FOUR_BIT_SCALE
                );
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, bulkSize, scores);
                return applyCorrections128Bulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    FOUR_BIT_SCALE,
                    FOUR_BIT_SCALE
                );
            }
        }
        return Float.NEGATIVE_INFINITY;
    }

}
