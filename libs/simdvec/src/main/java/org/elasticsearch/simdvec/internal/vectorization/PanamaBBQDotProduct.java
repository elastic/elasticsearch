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
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.lucene.store.IndexInputUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Panama-accelerated {@link BBQDotProduct}.
 */
public abstract class PanamaBBQDotProduct extends BBQDotProduct {

    // We need at least this size to fill a 128-bit vector
    private static final int MIN_PLANE_BYTES = 16;

    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
    private static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Long> LONG_SPECIES_128 = LongVector.SPECIES_128;
    private static final VectorSpecies<Long> LONG_SPECIES_256 = LongVector.SPECIES_256;

    private static final ValueLayout.OfLong LAYOUT_LE_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt LAYOUT_LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    public static boolean supports(IndexInput in, int docBits, int queryBits, int planeBytes) {
        return PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS
            && PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 128
            && planeBytes >= MIN_PLANE_BYTES
            && (queryBits == 1 || queryBits == 4)
            && docBits >= 1
            && docBits <= MAX_BITS
            && IndexInputUtils.canUseSegmentSlices(in);
    }

    /**
     * Factory method for a Panama dot-product implementation where possible.
     *
     * @param in         input positioned at the first data vector to score
     * @param nDims      number of dimensions
     * @param docBits    bits per dimension of the data vector, in {@code [1, MAX_BITS]}
     * @param queryBits  bits per dimension of the query vector, in {@code [1, MAX_BITS]}
     */
    public static BBQDotProduct create(IndexInput in, int nDims, int docBits, int queryBits) {
        int planeBytes = planeBytes(nDims);
        if (!supports(in, docBits, queryBits, planeBytes)) {
            return BBQDotProduct.create(in, nDims, docBits, queryBits);
        }
        return switch (queryBits) {
            case 1 -> new DxQ1Impl(in, docBits, planeBytes);
            case 4 -> new DxQ4Impl(in, docBits, planeBytes);
            default -> throw new AssertionError("unreachable, supports() restricts queryBits to 1 or 4: " + queryBits);
        };
    }

    private PanamaBBQDotProduct(IndexInput in, int docBits, int queryBits, int planeBytes) {
        super(in, docBits, queryBits, planeBytes);
    }

    /*
     * Here, each method is explicitly written out to eliminate virtual method calls in the hot loops, and to move
     * the vector size decision to as early as possible.
     */

    @Override
    public abstract long dotProduct(byte[] query) throws IOException;

    @Override
    public abstract void dotProductBulk(byte[] query, int count, float[] scores) throws IOException;

    @Override
    public abstract void dotProductBulkOffsets(byte[] query, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException;

    private static final class DxQ1Impl extends PanamaBBQDotProduct {

        DxQ1Impl(IndexInput in, int docBits, int planeBytes) {
            super(in, docBits, 1, planeBytes);
        }

        @Override
        public long dotProduct(byte[] query) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            return IndexInputUtils.withSlice(in, docBytes, scratch, segment -> {
                long dot = 0;
                if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    for (int dp = 0; dp < docBits; dp++) {
                        dot += dotProduct256(query, segment, (long) dp * planeBytes, planeBytes) << dp;
                    }
                } else {
                    for (int dp = 0; dp < docBits; dp++) {
                        dot += dotProduct128(query, segment, (long) dp * planeBytes, planeBytes) << dp;
                    }
                }
                return dot;
            });
        }

        @Override
        public void dotProductBulk(byte[] query, int count, float[] scores) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    for (int i = 0; i < count; i++) {
                        long offset = (long) i * docBytes;
                        long dot = 0;
                        for (int dp = 0; dp < docBits; dp++) {
                            dot += dotProduct256(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                        }
                        scores[i] = dot;
                    }
                });
            } else {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    for (int i = 0; i < count; i++) {
                        long offset = (long) i * docBytes;
                        long dot = 0;
                        for (int dp = 0; dp < docBits; dp++) {
                            dot += dotProduct128(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                        }
                        scores[i] = dot;
                    }
                });
            }
        }

        @Override
        public void dotProductBulkOffsets(byte[] query, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            // one slice covers the whole block, so skipped vectors are simply not scored
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    int next = 0;
                    for (int i = 0; i < count; i++) {
                        if (next < offsetsCount && offsets[next] == i) {
                            next++;

                            long offset = (long) i * docBytes;
                            long dot = 0;
                            for (int dp = 0; dp < docBits; dp++) {
                                dot += dotProduct256(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                            }
                            scores[i] = dot;
                        }
                    }
                });
            } else {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    int next = 0;
                    for (int i = 0; i < count; i++) {
                        if (next < offsetsCount && offsets[next] == i) {
                            next++;

                            long offset = (long) i * docBytes;
                            long dot = 0;
                            for (int dp = 0; dp < docBits; dp++) {
                                dot += dotProduct128(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                            }
                            scores[i] = dot;
                        }
                    }
                });
            }
        }

        private static long dotProduct256(byte[] q, MemorySegment segment, long baseOffset, int size) {
            long ret = 0;
            int i = 0;
            if (size >= BYTE_SPECIES_256.vectorByteSize() * 2) {
                int limit = BYTE_SPECIES_256.loopBound(size);
                var sum = LongVector.zero(LONG_SPECIES_256);
                for (; i < limit; i += BYTE_SPECIES_256.length()) {
                    var vq = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_256, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
                    sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
                }
                ret += sum.reduceLanes(VectorOperators.ADD);
            }
            if (size - i >= BYTE_SPECIES_128.vectorByteSize()) {
                int limit = BYTE_SPECIES_128.loopBound(size);
                var sum = LongVector.zero(LONG_SPECIES_128);
                for (; i < limit; i += BYTE_SPECIES_128.length()) {
                    var vq = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
                    sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
                }
                ret += sum.reduceLanes(VectorOperators.ADD);
            }

            // DON'T combine this with 128-bit impl, the extra method call slows it down
            for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
                final long value = segment.get(LAYOUT_LE_LONG, baseOffset + i);
                ret += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            }
            for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                final int value = segment.get(LAYOUT_LE_INT, baseOffset + i);
                ret += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            }
            for (; i < size; i++) {
                final int value = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i) & 0xFF;
                ret += Integer.bitCount((q[i] & value) & 0xFF);
            }
            return ret;
        }

        private static long dotProduct128(byte[] q, MemorySegment segment, long baseOffset, int size) {
            int i = 0;
            int limit = BYTE_SPECIES_128.loopBound(size);
            var sum = LongVector.zero(LONG_SPECIES_128);
            for (; i < limit; i += BYTE_SPECIES_128.length()) {
                var vq = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
                sum = sum.add(vq.and(vd).lanewise(VectorOperators.BIT_COUNT));
            }

            long ret = sum.reduceLanes(VectorOperators.ADD);

            // DON'T combine this with 256-bit impl, the extra method call slows it down
            for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
                final long value = segment.get(LAYOUT_LE_LONG, baseOffset + i);
                ret += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
            }
            for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                final int value = segment.get(LAYOUT_LE_INT, baseOffset + i);
                ret += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
            }
            for (; i < size; i++) {
                final int value = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i) & 0xFF;
                ret += Integer.bitCount((q[i] & value) & 0xFF);
            }
            return ret;
        }
    }

    private static final class DxQ4Impl extends PanamaBBQDotProduct {

        DxQ4Impl(IndexInput in, int docBits, int planeBytes) {
            super(in, docBits, 4, planeBytes);
        }

        @Override
        public long dotProduct(byte[] query) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            return IndexInputUtils.withSlice(in, docBytes, scratch, segment -> {
                long dot = 0;
                if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                    for (int dp = 0; dp < docBits; dp++) {
                        dot += dotProduct256(query, segment, (long) dp * planeBytes, planeBytes) << dp;
                    }
                } else {
                    for (int dp = 0; dp < docBits; dp++) {
                        dot += dotProduct128(query, segment, (long) dp * planeBytes, planeBytes) << dp;
                    }
                }
                return dot;
            });
        }

        @Override
        public void dotProductBulk(byte[] query, int count, float[] scores) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    for (int i = 0; i < count; i++) {
                        long offset = (long) i * docBytes;
                        long dot = 0;
                        for (int dp = 0; dp < docBits; dp++) {
                            dot += dotProduct256(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                        }
                        scores[i] = dot;
                    }
                });
            } else {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    for (int i = 0; i < count; i++) {
                        long offset = (long) i * docBytes;
                        long dot = 0;
                        for (int dp = 0; dp < docBits; dp++) {
                            dot += dotProduct128(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                        }
                        scores[i] = dot;
                    }
                });
            }
        }

        @Override
        public void dotProductBulkOffsets(byte[] query, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
            assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
            // one slice covers the whole block, so skipped vectors are simply not scored
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 256) {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    int next = 0;
                    for (int i = 0; i < count; i++) {
                        if (next < offsetsCount && offsets[next] == i) {
                            next++;

                            long offset = (long) i * docBytes;
                            long dot = 0;
                            for (int dp = 0; dp < docBits; dp++) {
                                dot += dotProduct256(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                            }
                            scores[i] = dot;
                        }
                    }
                });
            } else {
                IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, segment -> {
                    int next = 0;
                    for (int i = 0; i < count; i++) {
                        if (next < offsetsCount && offsets[next] == i) {
                            next++;

                            long offset = (long) i * docBytes;
                            long dot = 0;
                            for (int dp = 0; dp < docBits; dp++) {
                                dot += dotProduct128(query, segment, offset + (long) dp * planeBytes, planeBytes) << dp;
                            }
                            scores[i] = dot;
                        }
                    }
                });
            }
        }

        private static long dotProduct256(byte[] q, MemorySegment segment, long baseOffset, int size) {
            long subRet0 = 0;
            long subRet1 = 0;
            long subRet2 = 0;
            long subRet3 = 0;
            int i = 0;
            if (size >= BYTE_SPECIES_256.vectorByteSize() * 2) {
                int limit = BYTE_SPECIES_256.loopBound(size);
                var sum0 = LongVector.zero(LONG_SPECIES_256);
                var sum1 = LongVector.zero(LONG_SPECIES_256);
                var sum2 = LongVector.zero(LONG_SPECIES_256);
                var sum3 = LongVector.zero(LONG_SPECIES_256);
                for (; i < limit; i += BYTE_SPECIES_256.length()) {
                    var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                    var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size).reinterpretAsLongs();
                    var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 2).reinterpretAsLongs();
                    var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + size * 3).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_256, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
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
            if (size - i >= BYTE_SPECIES_128.vectorByteSize()) {
                int limit = BYTE_SPECIES_128.loopBound(size);
                var sum0 = LongVector.zero(LONG_SPECIES_128);
                var sum1 = LongVector.zero(LONG_SPECIES_128);
                var sum2 = LongVector.zero(LONG_SPECIES_128);
                var sum3 = LongVector.zero(LONG_SPECIES_128);
                for (; i < limit; i += BYTE_SPECIES_128.length()) {
                    var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                    var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size).reinterpretAsLongs();
                    var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 2).reinterpretAsLongs();
                    var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 3).reinterpretAsLongs();
                    var vd = LongVector.fromMemorySegment(LONG_SPECIES_128, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
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

            // DON'T combine this with 128-bit impl, the extra method call slows it down
            for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
                final long value = segment.get(LAYOUT_LE_LONG, baseOffset + i);
                subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
                subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
                subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
            }
            for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                final int value = segment.get(LAYOUT_LE_INT, baseOffset + i);
                subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
                subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
                subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
                subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
            }
            for (; i < size; i++) {
                final int value = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i) & 0xFF;
                subRet0 += Integer.bitCount((q[i] & value) & 0xFF);
                subRet1 += Integer.bitCount((q[i + size] & value) & 0xFF);
                subRet2 += Integer.bitCount((q[i + 2 * size] & value) & 0xFF);
                subRet3 += Integer.bitCount((q[i + 3 * size] & value) & 0xFF);
            }
            return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
        }

        private static long dotProduct128(byte[] q, MemorySegment segment, long baseOffset, int size) {
            int i = 0;
            var sum0 = IntVector.zero(INT_SPECIES_128);
            var sum1 = IntVector.zero(INT_SPECIES_128);
            var sum2 = IntVector.zero(INT_SPECIES_128);
            var sum3 = IntVector.zero(INT_SPECIES_128);
            int limit = BYTE_SPECIES_128.loopBound(size);
            for (; i < limit; i += BYTE_SPECIES_128.length()) {
                var vd = IntVector.fromMemorySegment(INT_SPECIES_128, segment, baseOffset + i, ByteOrder.LITTLE_ENDIAN);
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size).reinterpretAsInts();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 2).reinterpretAsInts();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + size * 3).reinterpretAsInts();
                sum0 = sum0.add(vd.and(vq0).lanewise(VectorOperators.BIT_COUNT));
                sum1 = sum1.add(vd.and(vq1).lanewise(VectorOperators.BIT_COUNT));
                sum2 = sum2.add(vd.and(vq2).lanewise(VectorOperators.BIT_COUNT));
                sum3 = sum3.add(vd.and(vq3).lanewise(VectorOperators.BIT_COUNT));
            }
            long subRet0 = sum0.reduceLanes(VectorOperators.ADD);
            long subRet1 = sum1.reduceLanes(VectorOperators.ADD);
            long subRet2 = sum2.reduceLanes(VectorOperators.ADD);
            long subRet3 = sum3.reduceLanes(VectorOperators.ADD);

            // DON'T combine this with 256-bit impl, the extra method call slows it down
            for (final int upperBound = size & -Long.BYTES; i < upperBound; i += Long.BYTES) {
                final long value = segment.get(LAYOUT_LE_LONG, baseOffset + i);
                subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i) & value);
                subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + size) & value);
                subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 2 * size) & value);
                subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, i + 3 * size) & value);
            }
            for (final int upperBound = size & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
                final int value = segment.get(LAYOUT_LE_INT, baseOffset + i);
                subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i) & value);
                subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + size) & value);
                subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 2 * size) & value);
                subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, i + 3 * size) & value);
            }
            for (; i < size; i++) {
                final int value = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i) & 0xFF;
                subRet0 += Integer.bitCount((q[i] & value) & 0xFF);
                subRet1 += Integer.bitCount((q[i + size] & value) & 0xFF);
                subRet2 += Integer.bitCount((q[i + 2 * size] & value) & 0xFF);
                subRet3 += Integer.bitCount((q[i + 3 * size] & value) & 0xFF);
            }
            return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
        }
    }
}
