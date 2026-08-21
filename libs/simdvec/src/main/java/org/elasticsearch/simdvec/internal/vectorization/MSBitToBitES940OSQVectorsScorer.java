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
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.lucene.store.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

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
        return IndexInputUtils.withSlice(in, length, scratch::get, segment -> bitDotProduct256(q, segment, length));
    }

    private long quantizeScore128(byte[] q) throws IOException {
        return IndexInputUtils.withSlice(in, length, scratch::get, segment -> bitDotProduct128(q, segment, length));
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
        IndexInputUtils.withSlice(in, datasetLengthInBytes, scratch::get, segment -> {
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
        IndexInputUtils.withSlice(in, datasetLengthInBytes, scratch::get, segment -> {
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
                return applyCorrectionsBulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    ONE_BIT_SCALE,
                    ONE_BIT_SCALE
                );
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 128) {
                quantizeScore128Bulk(q, bulkSize, scores);
                return applyCorrectionsBulk(
                    queryLowerInterval,
                    queryUpperInterval,
                    queryComponentSum,
                    queryAdditionalCorrection,
                    similarityFunction,
                    centroidDp,
                    scores,
                    bulkSize,
                    ONE_BIT_SCALE,
                    ONE_BIT_SCALE
                );
            }
        }
        return Float.NEGATIVE_INFINITY;
    }
}
