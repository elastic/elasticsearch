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
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.internal.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductD1Q4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductD1Q4Bulk;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductD1Q4BulkWithOffsets;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
final class MSBitToInt4ESNextOSQVectorsScorer extends MemorySegmentESNextOSQVectorsScorer.MemorySegmentScorer {

    MSBitToInt4ESNextOSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16) {
            if (NATIVE_SUPPORTED) {
                return nativeQuantizeScore(q);
            } else if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
                if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    return quantizeScore256(q);
                } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                    return quantizeScore128(q);
                }
            }
        }
        return Long.MIN_VALUE;
    }

    private long nativeQuantizeScore(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> nativeQuantizeScoreImpl(q, segment, length));
    }

    private static long nativeQuantizeScoreImpl(byte[] q, MemorySegment datasetMemorySegment, int length) {
        final long qScore;
        if (SUPPORTS_HEAP_SEGMENTS) {
            var queryMemorySegment = MemorySegment.ofArray(q);
            qScore = dotProductD1Q4(datasetMemorySegment, queryMemorySegment, length);
        } else {
            try (var arena = Arena.ofConfined()) {
                var queryMemorySegment = arena.allocate(q.length, 32);
                MemorySegment.copy(q, 0, queryMemorySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                qScore = dotProductD1Q4(datasetMemorySegment, queryMemorySegment, length);
            }
        }
        return qScore;
    }

    private long quantizeScore256(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> quantizeScore256Impl(q, segment, length));
    }

    private static long quantizeScore256Impl(byte[] q, MemorySegment memorySegment, int length) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        if (length >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
            int limit = ByteVector.SPECIES_256.loopBound(length);
            var sum0 = LongVector.zero(LONG_SPECIES_256);
            var sum1 = LongVector.zero(LONG_SPECIES_256);
            var sum2 = LongVector.zero(LONG_SPECIES_256);
            var sum3 = LongVector.zero(LONG_SPECIES_256);
            for (; i < limit; i += ByteVector.SPECIES_256.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 3).reinterpretAsLongs();
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

        if (length - i >= ByteVector.SPECIES_128.vectorByteSize()) {
            var sum0 = LongVector.zero(LONG_SPECIES_128);
            var sum1 = LongVector.zero(LONG_SPECIES_128);
            var sum2 = LongVector.zero(LONG_SPECIES_128);
            var sum3 = LongVector.zero(LONG_SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(length);
            for (; i < limit; i += ByteVector.SPECIES_128.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsLongs();
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
        for (final int upperBound = length & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = memorySegment.get(LAYOUT_LE_LONG, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
        }
        for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = memorySegment.get(LAYOUT_LE_INT, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
        }
        for (; i < length; i++) {
            final int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    private long quantizeScore128(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> quantizeScore128Impl(q, segment, length));
    }

    private static long quantizeScore128Impl(byte[] q, MemorySegment memorySegment, int length) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        var sum0 = IntVector.zero(INT_SPECIES_128);
        var sum1 = IntVector.zero(INT_SPECIES_128);
        var sum2 = IntVector.zero(INT_SPECIES_128);
        var sum3 = IntVector.zero(INT_SPECIES_128);
        int limit = ByteVector.SPECIES_128.loopBound(length);
        for (; i < limit; i += ByteVector.SPECIES_128.length()) {
            var vd = IntVector.fromMemorySegment(INT_SPECIES_128, memorySegment, i, ByteOrder.LITTLE_ENDIAN);
            var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
            var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsInts();
            var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsInts();
            var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsInts();
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
        for (final int upperBound = length & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = memorySegment.get(LAYOUT_LE_LONG, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
        }
        for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = memorySegment.get(LAYOUT_LE_INT, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
        }
        for (; i < length; i++) {
            final int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    @Override
    public boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16) {
            if (NATIVE_SUPPORTED) {
                if (SUPPORTS_HEAP_SEGMENTS) {
                    var querySegment = MemorySegment.ofArray(q);
                    var scoresSegment = MemorySegment.ofArray(scores);
                    nativeQuantizeScoreBulk(querySegment, count, scoresSegment);
                } else {
                    try (var arena = Arena.ofConfined()) {
                        var querySegment = arena.allocate(q.length, 32);
                        var scoresSegment = arena.allocate((long) scores.length * Float.BYTES, 32);
                        MemorySegment.copy(q, 0, querySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                        nativeQuantizeScoreBulk(querySegment, count, scoresSegment);
                        MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, scores.length);
                    }
                }
                return true;
            } else if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
                if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    quantizeScore256Bulk(q, count, scores);
                    return true;
                } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                    quantizeScore128Bulk(q, count, scores);
                    return true;
                }
            }
        }
        return false;
    }

    private void nativeQuantizeScoreBulk(MemorySegment querySegment, int count, MemorySegment scoresSegment) throws IOException {
        var datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, datasetSegment -> {
            dotProductD1Q4Bulk(datasetSegment, querySegment, length, count, scoresSegment);
            return null;
        });
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        var datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, segment -> {
            quantizeScore128BulkImpl(q, segment, length, count, scores);
            return null;
        });
    }

    private static void quantizeScore128BulkImpl(byte[] q, MemorySegment memorySegment, int length, int count, float[] scores) {
        int offset = 0;
        for (int iter = 0; iter < count; iter++) {
            long subRet0 = 0;
            long subRet1 = 0;
            long subRet2 = 0;
            long subRet3 = 0;
            int i = 0;
            var sum0 = IntVector.zero(INT_SPECIES_128);
            var sum1 = IntVector.zero(INT_SPECIES_128);
            var sum2 = IntVector.zero(INT_SPECIES_128);
            var sum3 = IntVector.zero(INT_SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(length);
            for (; i < limit; i += ByteVector.SPECIES_128.length(), offset += INT_SPECIES_128.vectorByteSize()) {
                var vd = IntVector.fromMemorySegment(INT_SPECIES_128, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsInts();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsInts();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsInts();
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
            for (final int upperBound = length & -Long.BYTES; i < upperBound; i += Long.BYTES, offset += Long.BYTES) {
                final long value = memorySegment.get(LAYOUT_LE_LONG, offset);
                subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
                subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
                subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
            }
            for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES, offset += Integer.BYTES) {
                final int value = memorySegment.get(LAYOUT_LE_INT, offset);
                subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
                subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
                subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
                subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
            }
            for (; i < length; i++, offset++) {
                final int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
                subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
                subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
                subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
            }
            scores[iter] = subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        var datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, segment -> {
            quantizeScore256BulkImpl(q, segment, length, count, scores);
            return null;
        });
    }

    private static void quantizeScore256BulkImpl(byte[] q, MemorySegment memorySegment, int length, int count, float[] scores) {
        int offset = 0;
        for (int iter = 0; iter < count; iter++) {
            long subRet0 = 0;
            long subRet1 = 0;
            long subRet2 = 0;
            long subRet3 = 0;
            int i = 0;
            if (length >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
                int limit = ByteVector.SPECIES_256.loopBound(length);
                var sum0 = LongVector.zero(LONG_SPECIES_256);
                var sum1 = LongVector.zero(LONG_SPECIES_256);
                var sum2 = LongVector.zero(LONG_SPECIES_256);
                var sum3 = LongVector.zero(LONG_SPECIES_256);
                for (; i < limit; i += ByteVector.SPECIES_256.length(), offset += LONG_SPECIES_256.vectorByteSize()) {
                    var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                    var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length).reinterpretAsLongs();
                    var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 2).reinterpretAsLongs();
                    var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 3).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_256, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
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

            if (length - i >= ByteVector.SPECIES_128.vectorByteSize()) {
                var sum0 = LongVector.zero(LONG_SPECIES_128);
                var sum1 = LongVector.zero(LONG_SPECIES_128);
                var sum2 = LongVector.zero(LONG_SPECIES_128);
                var sum3 = LongVector.zero(LONG_SPECIES_128);
                int limit = ByteVector.SPECIES_128.loopBound(length);
                for (; i < limit; i += ByteVector.SPECIES_128.length(), offset += LONG_SPECIES_128.vectorByteSize()) {
                    var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                    var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsLongs();
                    var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsLongs();
                    var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
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
            for (final int upperBound = length & -Long.BYTES; i < upperBound; i += Long.BYTES, offset += Long.BYTES) {
                final long value = memorySegment.get(LAYOUT_LE_LONG, offset);
                subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
                subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
                subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
            }
            for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES, offset += Integer.BYTES) {
                final int value = memorySegment.get(LAYOUT_LE_INT, offset);
                subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
                subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
                subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
                subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
            }
            for (; i < length; i++, offset++) {
                final int dValue = memorySegment.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
                subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
                subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
                subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
                subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
            }
            scores[iter] = subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
        }
    }

    @Override
    public boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        assert q.length == length * 4;
        if (NATIVE_SUPPORTED) {
            if (SUPPORTS_HEAP_SEGMENTS) {
                var querySegment = MemorySegment.ofArray(q);
                var offsetsSegment = MemorySegment.ofArray(offsets);
                var scoresSegment = MemorySegment.ofArray(scores);
                nativeQuantizeScoreBulkOffsets(querySegment, offsetsSegment, scoresSegment, offsetsCount, count);
            } else {
                try (var arena = Arena.ofConfined()) {
                    var querySegment = arena.allocate(q.length, 32);
                    var offsetsSegment = arena.allocate((long) offsetsCount * Integer.BYTES, 32);
                    var scoresSegment = arena.allocate((long) scores.length * Float.BYTES, 32);
                    MemorySegment.copy(q, 0, querySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                    MemorySegment.copy(offsets, 0, offsetsSegment, ValueLayout.JAVA_INT, 0, offsetsCount);
                    nativeQuantizeScoreBulkOffsets(querySegment, offsetsSegment, scoresSegment, offsetsCount, count);
                    MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, scores.length);
                }
            }
            repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
            return true;
        }
        return false;
    }

    private void nativeQuantizeScoreBulkOffsets(
        MemorySegment querySegment,
        MemorySegment offsetsSegment,
        MemorySegment scoresSegment,
        int offsetsCount,
        int totalCount
    ) throws IOException {
        var datasetLengthInBytes = (long) length * totalCount;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, datasetSegment -> {
            dotProductD1Q4BulkWithOffsets(datasetSegment, querySegment, length, length, offsetsSegment, offsetsCount, scoresSegment);
            return null;
        });
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
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16) {
            if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
                if (NATIVE_SUPPORTED) {
                    if (SUPPORTS_HEAP_SEGMENTS) {
                        var querySegment = MemorySegment.ofArray(q);
                        var scoresSegment = MemorySegment.ofArray(scores);
                        nativeQuantizeScoreBulk(querySegment, bulkSize, scoresSegment);
                        return nativeApplyCorrectionsBulk(
                            queryLowerInterval,
                            queryUpperInterval,
                            queryComponentSum,
                            queryAdditionalCorrection,
                            similarityFunction,
                            centroidDp,
                            scoresSegment,
                            bulkSize
                        );
                    } else {
                        try (var arena = Arena.ofConfined()) {
                            var querySegment = arena.allocate(q.length, 32);
                            var scoresSegment = arena.allocate((long) scores.length * Float.BYTES, 32);
                            MemorySegment.copy(q, 0, querySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                            nativeQuantizeScoreBulk(querySegment, bulkSize, scoresSegment);
                            var maxScore = nativeApplyCorrectionsBulk(
                                queryLowerInterval,
                                queryUpperInterval,
                                queryComponentSum,
                                queryAdditionalCorrection,
                                similarityFunction,
                                centroidDp,
                                scoresSegment,
                                bulkSize
                            );
                            MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, scores.length);
                            return maxScore;
                        }
                    }
                } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    quantizeScore256Bulk(q, bulkSize, scores);
                    return applyCorrections256Bulk(
                        queryLowerInterval,
                        queryUpperInterval,
                        queryComponentSum,
                        queryAdditionalCorrection,
                        similarityFunction,
                        centroidDp,
                        scores,
                        bulkSize
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
                        bulkSize
                    );
                }
            }
        }
        return Float.NEGATIVE_INFINITY;
    }

    private float nativeApplyCorrectionsBulk(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        MemorySegment scoresSegment,
        int bulkSize
    ) throws IOException {
        return IndexInputUtils.withSlice(
            in,
            16L * bulkSize,
            this::getScratch,
            seg -> ScoreCorrections.nativeApplyCorrectionsBulk(
                similarityFunction,
                seg,
                bulkSize,
                dimensions,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                FOUR_BIT_SCALE,
                ONE_BIT_SCALE,
                centroidDp,
                scoresSegment
            )
        );
    }

    private float applyCorrections128Bulk(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        return IndexInputUtils.withSlice(
            in,
            16L * bulkSize,
            this::getScratch,
            seg -> applyCorrections128BulkImpl(
                seg,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                bulkSize,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum
            )
        );
    }

    private float applyCorrections128BulkImpl(
        MemorySegment memorySegment,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum
    ) {
        int limit = FLOAT_SPECIES_128.loopBound(bulkSize);
        int i = 0;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_128.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES_128, memorySegment, i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_128,
                memorySegment,
                4L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES_128,
                memorySegment,
                8L * bulkSize + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_128,
                memorySegment,
                12L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            );
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES_128, scores, i);
            // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly *
            // qcDist;
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_128, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
                    // not sure how to do it better
                    for (int j = 0; j < FLOAT_SPECIES_128.length(); j++) {
                        scores[i + j] = VectorUtil.scaleMaxInnerProductScore(scores[i + j]);
                        maxScore = Math.max(maxScore, scores[i + j]);
                    }
                } else {
                    res = res.add(1f).mul(0.5f).max(0);
                    res.intoArray(scores, i);
                    maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                }
            }
        }
        if (limit < bulkSize) {
            maxScore = applyCorrectionsIndividually(
                memorySegment,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                ONE_BIT_SCALE,
                scores,
                bulkSize,
                limit,
                ay,
                ly,
                y1,
                maxScore
            );
        }
        return maxScore;
    }

    private float applyCorrections256Bulk(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize
    ) throws IOException {
        return IndexInputUtils.withSlice(
            in,
            16L * bulkSize,
            this::getScratch,
            memorySegment -> applyCorrections256BulkImpl(
                memorySegment,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                bulkSize,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum
            )
        );
    }

    private float applyCorrections256BulkImpl(
        MemorySegment memorySegment,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum
    ) {
        int limit = FLOAT_SPECIES_256.loopBound(bulkSize);
        int i = 0;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_256.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES_256, memorySegment, i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_256,
                memorySegment,
                4L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES_256,
                memorySegment,
                8L * bulkSize + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_256,
                memorySegment,
                12L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            );
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES_256, scores, i);
            // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly *
            // qcDist;
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_256, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
                    // not sure how to do it better
                    for (int j = 0; j < FLOAT_SPECIES_256.length(); j++) {
                        scores[i + j] = VectorUtil.scaleMaxInnerProductScore(scores[i + j]);
                        maxScore = Math.max(maxScore, scores[i + j]);
                    }
                } else {
                    res = res.add(1f).mul(0.5f).max(0);
                    maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                    res.intoArray(scores, i);
                }
            }
        }
        if (limit < bulkSize) {
            maxScore = applyCorrectionsIndividually(
                memorySegment,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                ONE_BIT_SCALE,
                scores,
                bulkSize,
                limit,
                ay,
                ly,
                y1,
                maxScore
            );
        }
        return maxScore;
    }
}
