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
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for quantized vectors stored as a {@link IndexInput}. */
public final class OnHeapES91OSQVectorsScorer extends ES91OSQVectorsScorer {

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;

    private static final VectorSpecies<Long> LONG_SPECIES_128 = LongVector.SPECIES_128;
    private static final VectorSpecies<Long> LONG_SPECIES_256 = LongVector.SPECIES_256;

    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
    private static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;

    private static final VectorSpecies<Float> FLOAT_SPECIES_128 = FloatVector.SPECIES_128;
    private static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;

    private final byte[] bytes;

    public OnHeapES91OSQVectorsScorer(IndexInput in, int dimensions) {
        super(in, dimensions);
        bytes = new byte[BULK_SIZE * length];
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return quantizeScore256(q);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return quantizeScore128(q);
            }
        }
        return super.quantizeScore(q);
    }

    private long quantizeScore256(byte[] q) throws IOException {
        in.readBytes(bytes, 0, length);
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        if (length >= BYTE_SPECIES_256.vectorByteSize() * 2) {
            int limit = BYTE_SPECIES_256.loopBound(length);
            var sum0 = LongVector.zero(LONG_SPECIES_256);
            var sum1 = LongVector.zero(LONG_SPECIES_256);
            var sum2 = LongVector.zero(LONG_SPECIES_256);
            var sum3 = LongVector.zero(LONG_SPECIES_256);
            for (; i < limit; i += BYTE_SPECIES_256.length()) {
                var vd = ByteVector.fromArray(BYTE_SPECIES_256, bytes, i).reinterpretAsLongs();
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 3).reinterpretAsLongs();
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

        if (length - i >= BYTE_SPECIES_128.vectorByteSize()) {
            var sum0 = LongVector.zero(LONG_SPECIES_128);
            var sum1 = LongVector.zero(LONG_SPECIES_128);
            var sum2 = LongVector.zero(LONG_SPECIES_128);
            var sum3 = LongVector.zero(LONG_SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(length);
            for (; i < limit; i += BYTE_SPECIES_128.length()) {
                var vd = ByteVector.fromArray(BYTE_SPECIES_128, bytes, i).reinterpretAsLongs();
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsLongs();
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
            final long value = (long) BitUtil.VH_LE_LONG.get(bytes, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
        }
        for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = (int) BitUtil.VH_LE_INT.get(bytes, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
        }
        for (; i < length; i++) {
            int dValue = bytes[i] & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    private long quantizeScore128(byte[] q) throws IOException {
        in.readBytes(bytes, 0, length);
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        var sum0 = IntVector.zero(INT_SPECIES_128);
        var sum1 = IntVector.zero(INT_SPECIES_128);
        var sum2 = IntVector.zero(INT_SPECIES_128);
        var sum3 = IntVector.zero(INT_SPECIES_128);
        int limit = BYTE_SPECIES_128.loopBound(length);
        int i = 0;
        for (; i < limit; i += BYTE_SPECIES_128.length()) {
            var vd = ByteVector.fromArray(BYTE_SPECIES_128, bytes, i).reinterpretAsInts();
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
            final long value = (long) BitUtil.VH_LE_LONG.get(bytes, i);
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
        }
        for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = (int) BitUtil.VH_LE_INT.get(bytes, i);
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
        }
        for (; i < length; i++) {
            int dValue = bytes[i] & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    @Override
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                quantizeScore256Bulk(q, count, scores);
                return;
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, count, scores);
                return;
            }
        }
        super.quantizeScoreBulk(q, count, scores);
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        int j = 0;
        for (; j < count - 15; j += BULK_SIZE) {
            in.readBytes(bytes, 0, BULK_SIZE * length);
            for (int iter = 0; iter < BULK_SIZE; iter++) {
                long subRet0 = 0;
                long subRet1 = 0;
                long subRet2 = 0;
                long subRet3 = 0;
                var sum0 = IntVector.zero(INT_SPECIES_128);
                var sum1 = IntVector.zero(INT_SPECIES_128);
                var sum2 = IntVector.zero(INT_SPECIES_128);
                var sum3 = IntVector.zero(INT_SPECIES_128);
                int limit = ByteVector.SPECIES_128.loopBound(length);
                int i = 0;
                for (; i < limit; i += ByteVector.SPECIES_128.length()) {
                    var vd = ByteVector.fromArray(BYTE_SPECIES_128, bytes, iter * length + i).reinterpretAsInts();
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
                    final long value = (long) BitUtil.VH_LE_LONG.get(bytes, iter * length + i);
                    subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                    subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
                    subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
                    subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
                }
                for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                    final int value = (int) BitUtil.VH_LE_INT.get(bytes, iter * length + i);
                    subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
                    subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
                    subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
                    subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
                }
                for (; i < length; i++) {
                    int dValue = bytes[iter * length + i] & 0xFF;
                    subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
                    subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
                    subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
                    subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
                }
                scores[j + iter] = subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
            }
        }
        for (; j < count; j++) {
            scores[j] = quantizeScore128(q);
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        int j = 0;
        for (; j < count - 15; j += BULK_SIZE) {
            in.readBytes(bytes, 0, BULK_SIZE * length);
            for (int iter = 0; iter < BULK_SIZE; iter++) {
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
                        var vd = ByteVector.fromArray(BYTE_SPECIES_256, bytes, iter * length + i).reinterpretAsLongs();
                        var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                        var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length).reinterpretAsLongs();
                        var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 2).reinterpretAsLongs();
                        var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + length * 3).reinterpretAsLongs();
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
                        var vd = ByteVector.fromArray(BYTE_SPECIES_128, bytes, iter * length + i).reinterpretAsLongs();
                        var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                        var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length).reinterpretAsLongs();
                        var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 2).reinterpretAsLongs();
                        var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + length * 3).reinterpretAsLongs();
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
                    final long value = (long) BitUtil.VH_LE_LONG.get(bytes, iter * length + i);
                    subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                    subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + length) & value);
                    subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * length) & value);
                    subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * length) & value);
                }
                for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                    final int value = (int) BitUtil.VH_LE_INT.get(bytes, i);
                    subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, iter * length + i) & value);
                    subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + length) & value);
                    subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * length) & value);
                    subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * length) & value);
                }
                for (; i < length; i++) {
                    int dValue = bytes[iter * length + i] & 0xFF;
                    subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
                    subRet1 += Integer.bitCount((q[i + length] & dValue) & 0xFF);
                    subRet2 += Integer.bitCount((q[i + 2 * length] & dValue) & 0xFF);
                    subRet3 += Integer.bitCount((q[i + 3 * length] & dValue) & 0xFF);
                }
                scores[j + iter] = subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
            }
        }
        for (; j < count; j++) {
            scores[j] = quantizeScore256(q);
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
        float[] scores
    ) throws IOException {
        assert q.length == length * 4;
        // 128 / 8 == 16
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return score256Bulk(
                    q,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores
                );
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return score128Bulk(
                    q,
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores
                );
            }
        }
        return super.scoreBulk(
            q,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            scores
        );
    }

    private float score128Bulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        quantizeScore128Bulk(q, BULK_SIZE, scores);
        in.readFloats(lowerIntervals, 0, BULK_SIZE);
        in.readFloats(upperIntervals, 0, BULK_SIZE);
        for (int i = 0; i < BULK_SIZE; i++) {
            targetComponentSums[i] = Short.toUnsignedInt(in.readShort());
        }
        in.readFloats(additionalCorrections, 0, BULK_SIZE);
        int limit = FLOAT_SPECIES_128.loopBound(BULK_SIZE);
        int i = 0;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_128.length()) {
            var ax = FloatVector.fromArray(FLOAT_SPECIES_128, lowerIntervals, i);
            var lx = FloatVector.fromArray(FLOAT_SPECIES_128, upperIntervals, i).sub(ax);
            var targetComponentSumsVect = IntVector.fromArray(INT_SPECIES_128, targetComponentSums, i).convert(VectorOperators.I2F, 0);
            var additionalCorrectionsVect = FloatVector.fromArray(FLOAT_SPECIES_128, additionalCorrections, i);
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES_128, scores, i);
            // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly *
            // qcDist;
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSumsVect);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrectionsVect).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_128, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(additionalCorrectionsVect).add(queryAdditionalCorrection).sub(centroidDp);
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
        return maxScore;
    }

    private float score256Bulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        quantizeScore256Bulk(q, BULK_SIZE, scores);
        in.readFloats(lowerIntervals, 0, BULK_SIZE);
        in.readFloats(upperIntervals, 0, BULK_SIZE);
        for (int i = 0; i < BULK_SIZE; i++) {
            targetComponentSums[i] = Short.toUnsignedInt(in.readShort());
        }
        in.readFloats(additionalCorrections, 0, BULK_SIZE);
        int limit = FLOAT_SPECIES_256.loopBound(BULK_SIZE);
        int i = 0;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_256.length()) {
            var ax = FloatVector.fromArray(FLOAT_SPECIES_256, lowerIntervals, i);
            var lx = FloatVector.fromArray(FLOAT_SPECIES_256, upperIntervals, i).sub(ax);
            var targetComponentSumsVect = IntVector.fromArray(INT_SPECIES_256, targetComponentSums, i).convert(VectorOperators.I2F, 0);
            var additionalCorrectionsVect = FloatVector.fromArray(FLOAT_SPECIES_256, additionalCorrections, i);
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES_256, scores, i);
            // ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly *
            // qcDist;
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSumsVect);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            // For euclidean, we need to invert the score and apply the additional correction, which is
            // assumed to be the squared l2norm of the centroid centered vectors.
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrectionsVect).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_256, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(queryAdditionalCorrection).add(additionalCorrectionsVect).sub(centroidDp);
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
        return maxScore;
    }
}
