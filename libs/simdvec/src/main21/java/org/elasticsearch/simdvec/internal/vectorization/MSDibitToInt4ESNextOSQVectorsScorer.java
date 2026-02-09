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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductD2Q4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductD2Q4Bulk;

/** Panamized scorer for quantized vectors stored as a {@link MemorySegment}. */
final class MSDibitToInt4ESNextOSQVectorsScorer extends MemorySegmentESNextOSQVectorsScorer.MemorySegmentScorer {

    MSDibitToInt4ESNextOSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize, MemorySegment memorySegment) {
        super(in, dimensions, dataLength, bulkSize, memorySegment);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length * 2;
        // 128 / 8 == 16
        if (length >= 16) {
            if (NATIVE_SUPPORTED) {
                return nativeQuantizeScore(q);
            } else if (PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
                if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    return quantizeScore256DibitToInt4(q);
                } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                    return quantizeScore128DibitToInt4(q);
                }
            }
        }
        return Long.MIN_VALUE;
    }

    private long nativeQuantizeScore(byte[] q) throws IOException {
        long offset = in.getFilePointer();
        var datasetMemorySegment = memorySegment.asSlice(offset, length);

        final long qScore;
        if (SUPPORTS_HEAP_SEGMENTS) {
            var queryMemorySegment = MemorySegment.ofArray(q);
            qScore = dotProductD2Q4(datasetMemorySegment, queryMemorySegment, length);
        } else {
            try (var arena = Arena.ofConfined()) {
                var queryMemorySegment = arena.allocate(q.length, 32);
                MemorySegment.copy(q, 0, queryMemorySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                qScore = dotProductD2Q4(datasetMemorySegment, queryMemorySegment, length);
            }
        }
        in.skipBytes(length);
        return qScore;
    }

    private long quantizeScore256DibitToInt4(byte[] q) throws IOException {
        int lower = (int) quantizeScore256(q);
        int upper = (int) quantizeScore256(q);
        return lower + ((long) upper << 1);
    }

    private long quantizeScore128DibitToInt4(byte[] q) throws IOException {
        int lower = (int) quantizeScore128(q);
        int upper = (int) quantizeScore128(q);
        return lower + ((long) upper << 1);
    }

    private long quantizeScore256(byte[] q) throws IOException {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        long offset = in.getFilePointer();
        int size = length / 2;
        if (size >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
            int limit = ByteVector.SPECIES_256.loopBound(size);
            var sum0 = LongVector.zero(LONG_SPECIES_256);
            var sum1 = LongVector.zero(LONG_SPECIES_256);
            var sum2 = LongVector.zero(LONG_SPECIES_256);
            var sum3 = LongVector.zero(LONG_SPECIES_256);
            for (; i < limit; i += ByteVector.SPECIES_256.length(), offset += LONG_SPECIES_256.vectorByteSize()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 3).reinterpretAsLongs();
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

        if (size - i >= ByteVector.SPECIES_128.vectorByteSize()) {
            var sum0 = LongVector.zero(LONG_SPECIES_128);
            var sum1 = LongVector.zero(LONG_SPECIES_128);
            var sum2 = LongVector.zero(LONG_SPECIES_128);
            var sum3 = LongVector.zero(LONG_SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(size);
            for (; i < limit; i += ByteVector.SPECIES_128.length(), offset += LONG_SPECIES_128.vectorByteSize()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 3).reinterpretAsLongs();
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
        in.seek(offset);
        for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = in.readLong();
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = in.readInt();
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
        }
        for (; i < size; i++) {
            int dValue = in.readByte() & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + size] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * size] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * size] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    private long quantizeScore128(byte[] q) throws IOException {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;
        long offset = in.getFilePointer();

        var sum0 = IntVector.zero(INT_SPECIES_128);
        var sum1 = IntVector.zero(INT_SPECIES_128);
        var sum2 = IntVector.zero(INT_SPECIES_128);
        var sum3 = IntVector.zero(INT_SPECIES_128);
        int size = length / 2;
        int limit = ByteVector.SPECIES_128.loopBound(size);
        for (; i < limit; i += ByteVector.SPECIES_128.length(), offset += INT_SPECIES_128.vectorByteSize()) {
            var vd = IntVector.fromMemorySegment(INT_SPECIES_128, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
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
        in.seek(offset);
        for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = in.readLong();
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = in.readInt();
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
        }
        for (; i < size; i++) {
            int dValue = in.readByte() & 0xFF;
            subRet0 += Integer.bitCount((q[i] & dValue) & 0xFF);
            subRet1 += Integer.bitCount((q[i + size] & dValue) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * size] & dValue) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * size] & dValue) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    @Override
    public boolean quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        assert q.length == length * 2;
        // 128 / 8 == 16
        if (length >= 16) {
            if (NATIVE_SUPPORTED) {
                if (SUPPORTS_HEAP_SEGMENTS) {
                    var queryMemorySegment = MemorySegment.ofArray(q);
                    var scoresSegment = MemorySegment.ofArray(scores);
                    nativeQuantizeScoreBulk(queryMemorySegment, count, scoresSegment);
                } else {
                    try (var arena = Arena.ofConfined()) {
                        var queryMemorySegment = arena.allocate(q.length, 32);
                        var scoresSegment = arena.allocate((long) scores.length * Float.BYTES, 32);
                        MemorySegment.copy(q, 0, queryMemorySegment, ValueLayout.JAVA_BYTE, 0, q.length);
                        nativeQuantizeScoreBulk(queryMemorySegment, count, scoresSegment);
                        MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, scores.length);
                    }
                }
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

    private void nativeQuantizeScoreBulk(MemorySegment queryMemorySegment, int count, MemorySegment scoresSegment) throws IOException {
        long initialOffset = in.getFilePointer();
        var datasetLengthInBytes = (long) length * count;
        MemorySegment datasetSegment = memorySegment.asSlice(initialOffset, datasetLengthInBytes);

        dotProductD2Q4Bulk(datasetSegment, queryMemorySegment, length, count, scoresSegment);

        in.skipBytes(datasetLengthInBytes);
    }

    private void quantizeScore128Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = quantizeScore128DibitToInt4(q);
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = quantizeScore256DibitToInt4(q);
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
        assert q.length == length * 2;
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
        int limit = FLOAT_SPECIES_128.loopBound(bulkSize);
        int i = 0;
        long offset = in.getFilePointer();
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_128.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES_128, memorySegment, offset + i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_128,
                memorySegment,
                offset + 4L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax).mul(TWO_BIT_SCALE);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES_128,
                memorySegment,
                offset + 8L * bulkSize + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_128,
                memorySegment,
                offset + 12L * bulkSize + i * Float.BYTES,
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
            // missing vectors to score
            maxScore = applyCorrectionsIndividually(
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                TWO_BIT_SCALE,
                scores,
                bulkSize,
                limit,
                offset,
                ay,
                ly,
                y1,
                maxScore
            );
        }
        in.seek(offset + 16L * bulkSize);
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
        int limit = FLOAT_SPECIES_256.loopBound(bulkSize);
        int i = 0;
        long offset = in.getFilePointer();
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryComponentSum;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (; i < limit; i += FLOAT_SPECIES_256.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES_256, memorySegment, offset + i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_256,
                memorySegment,
                offset + 4L * bulkSize + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax).mul(TWO_BIT_SCALE);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES_256,
                memorySegment,
                offset + 8L * bulkSize + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES_256,
                memorySegment,
                offset + 12L * bulkSize + i * Float.BYTES,
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
            // missing vectors to score
            maxScore = applyCorrectionsIndividually(
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                TWO_BIT_SCALE,
                scores,
                bulkSize,
                limit,
                offset,
                ay,
                ly,
                y1,
                maxScore
            );
        }
        in.seek(offset + 16L * bulkSize);
        return maxScore;
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
        long offset = in.getFilePointer();

        final float maxScore = ScoreCorrections.nativeApplyCorrectionsBulk(
            similarityFunction,
            memorySegment.asSlice(offset),
            bulkSize,
            dimensions,
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            FOUR_BIT_SCALE,
            TWO_BIT_SCALE,
            centroidDp,
            scoresSegment
        );
        in.seek(offset + 16L * bulkSize);
        return maxScore;
    }
}
