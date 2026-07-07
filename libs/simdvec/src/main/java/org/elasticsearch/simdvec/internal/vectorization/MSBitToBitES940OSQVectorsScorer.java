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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for symmetric 1-bit query and 1-bit index vectors. */
final class MSBitToBitES940OSQVectorsScorer extends MemorySegmentES940OSQVectorsScorer.MemorySegmentScorer {

    MSBitToBitES940OSQVectorsScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    public long quantizeScore(byte[] q) throws IOException {
        assert q.length == length;
        if (length >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                return quantizeScore256(q);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                return quantizeScore128(q);
            }
        }
        return Long.MIN_VALUE;
    }

    private long quantizeScore256(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> bitDotProduct256(q, segment, length));
    }

    private long quantizeScore128(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, this::getScratch, segment -> bitDotProduct128(q, segment, length));
    }

    private static long bitDotProduct256(byte[] q, MemorySegment d, int length) {
        long ret = 0;
        int i = 0;
        if (length >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
            int limit = ByteVector.SPECIES_256.loopBound(length);
            var sum = LongVector.zero(LONG_SPECIES_256);
            for (; i < limit; i += ByteVector.SPECIES_256.length()) {
                var vq = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vd = LongVector.fromMemorySegment(LONG_SPECIES_256, d, i, ByteOrder.LITTLE_ENDIAN);
                sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
            }
            ret += sum.reduceLanes(VectorOperators.ADD);
        }

        if (length - i >= ByteVector.SPECIES_128.vectorByteSize()) {
            int limit = ByteVector.SPECIES_128.loopBound(length);
            var sum = LongVector.zero(LONG_SPECIES_128);
            for (; i < limit; i += ByteVector.SPECIES_128.length()) {
                var vq = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, d, i, ByteOrder.LITTLE_ENDIAN);
                sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
            }
            ret += sum.reduceLanes(VectorOperators.ADD);
        }

        ret += bitDotProductScalarTail(q, d, i, length);
        return ret;
    }

    private static long bitDotProduct128(byte[] q, MemorySegment d, int length) {
        long ret = 0;
        int i = 0;
        int limit = ByteVector.SPECIES_128.loopBound(length);
        var sum = LongVector.zero(LONG_SPECIES_128);
        for (; i < limit; i += ByteVector.SPECIES_128.length()) {
            var vq = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
            var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, d, i, ByteOrder.LITTLE_ENDIAN);
            sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
        }
        ret += sum.reduceLanes(VectorOperators.ADD);
        ret += bitDotProductScalarTail(q, d, i, length);
        return ret;
    }

    private static long bitDotProductScalarTail(byte[] q, MemorySegment d, int i, int length) {
        long ret = 0;
        for (final int upperBound = length & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            final long value = d.get(LAYOUT_LE_LONG, i);
            ret += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
        }
        for (final int upperBound = length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            final int value = d.get(LAYOUT_LE_INT, i);
            ret += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
        }
        for (; i < length; i++) {
            final int dValue = d.get(ValueLayout.JAVA_BYTE, i) & 0xFF;
            ret += Integer.bitCount((q[i] & dValue) & 0xFF);
        }
        return ret;
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
        long datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, segment -> {
            quantizeScore128BulkImpl(q, segment, length, count, scores);
            return null;
        });
    }

    private static void quantizeScore128BulkImpl(byte[] q, MemorySegment d, int length, int count, float[] scores) {
        int offset = 0;
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = bitDotProduct128(q, d.asSlice(offset, length), length);
            offset += length;
        }
    }

    private void quantizeScore256Bulk(byte[] q, int count, float[] scores) throws IOException {
        long datasetLengthInBytes = (long) length * count;
        IndexInputUtils.withSlice(in, datasetLengthInBytes, this::getScratch, segment -> {
            quantizeScore256BulkImpl(q, segment, length, count, scores);
            return null;
        });
    }

    private static void quantizeScore256BulkImpl(byte[] q, MemorySegment d, int length, int count, float[] scores) {
        int offset = 0;
        for (int iter = 0; iter < count; iter++) {
            scores[iter] = bitDotProduct256(q, d.asSlice(offset, length), length);
            offset += length;
        }
    }

    @Override
    public boolean quantizeScoreBulkOffsets(byte[] q, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        return false;
    }

    @Override
    float scoreBulkOffsets(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int[] offsets,
        int offsetsCount,
        float[] scores,
        int count
    ) {
        return Float.NEGATIVE_INFINITY;
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
        float ly = (queryUpperInterval - ay) * ONE_BIT_SCALE;
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
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_128, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
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
        float ly = (queryUpperInterval - ay) * ONE_BIT_SCALE;
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
            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);
            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES_256, 1).div(res).max(0);
                maxScore = Math.max(maxScore, res.reduceLanes(VectorOperators.MAX));
                res.intoArray(scores, i);
            } else {
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
                    for (int j = 0; j < FLOAT_SPECIES_256.length(); j++) {
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
}
