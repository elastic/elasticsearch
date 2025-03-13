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
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Constants;

public final class PanamaESVectorUtilSupport implements ESVectorUtilSupport {

    static final int VECTOR_BITSIZE;

    /** Whether integer vectors can be trusted to actually be fast. */
    static final boolean HAS_FAST_INTEGER_VECTORS;

    static {
        // default to platform supported bitsize
        VECTOR_BITSIZE = VectorShape.preferredShape().vectorBitSize();

        // hotspot misses some SSE intrinsics, workaround it
        // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
        boolean isAMD64withoutAVX2 = Constants.OS_ARCH.equals("amd64") && VECTOR_BITSIZE < 256;
        HAS_FAST_INTEGER_VECTORS = isAMD64withoutAVX2 == false;
    }

    @Override
    public long ipByteBinByte(byte[] q, byte[] d) {
        // 128 / 8 == 16
        if (d.length >= 16 && HAS_FAST_INTEGER_VECTORS) {
            if (VECTOR_BITSIZE >= 256) {
                return ipByteBin256(q, d);
            } else if (VECTOR_BITSIZE == 128) {
                return ipByteBin128(q, d);
            }
        }
        return DefaultESVectorUtilSupport.ipByteBinByteImpl(q, d);
    }

    @Override
    public int ipByteBit(byte[] q, byte[] d) {
        if (d.length >= 16 && HAS_FAST_INTEGER_VECTORS) {
            if (VECTOR_BITSIZE >= 512) {
                return ipByteBit512(q, d);
            } else if (VECTOR_BITSIZE == 256) {
                return ipByteBit256(q, d);
            }
        }
        return DefaultESVectorUtilSupport.ipByteBitImpl(q, d);
    }

    @Override
    public float ipFloatBit(float[] q, byte[] d) {
        if (q.length >= 16) {
            if (VECTOR_BITSIZE >= 512) {
                return ipFloatBit512(q, d);
            } else if (VECTOR_BITSIZE == 256) {
                return ipFloatBit256(q, d);
            }
        }
        return DefaultESVectorUtilSupport.ipFloatBitImpl(q, d);
    }

    @Override
    public float ipFloatByte(float[] q, byte[] d) {
        return DefaultESVectorUtilSupport.ipFloatByteImpl(q, d);
    }

    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;
    private static final VectorSpecies<Byte> BYTE_SPECIES_256 = ByteVector.SPECIES_256;

    static long ipByteBin256(byte[] q, byte[] d) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;

        if (d.length >= ByteVector.SPECIES_256.vectorByteSize() * 2) {
            int limit = ByteVector.SPECIES_256.loopBound(d.length);
            var sum0 = LongVector.zero(LongVector.SPECIES_256);
            var sum1 = LongVector.zero(LongVector.SPECIES_256);
            var sum2 = LongVector.zero(LongVector.SPECIES_256);
            var sum3 = LongVector.zero(LongVector.SPECIES_256);
            for (; i < limit; i += ByteVector.SPECIES_256.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_256, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + d.length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + d.length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_256, q, i + d.length * 3).reinterpretAsLongs();
                var vd = ByteVector.fromArray(BYTE_SPECIES_256, d, i).reinterpretAsLongs();
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

        if (d.length - i >= ByteVector.SPECIES_128.vectorByteSize()) {
            var sum0 = LongVector.zero(LongVector.SPECIES_128);
            var sum1 = LongVector.zero(LongVector.SPECIES_128);
            var sum2 = LongVector.zero(LongVector.SPECIES_128);
            var sum3 = LongVector.zero(LongVector.SPECIES_128);
            int limit = ByteVector.SPECIES_128.loopBound(d.length);
            for (; i < limit; i += ByteVector.SPECIES_128.length()) {
                var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsLongs();
                var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length).reinterpretAsLongs();
                var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length * 2).reinterpretAsLongs();
                var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length * 3).reinterpretAsLongs();
                var vd = ByteVector.fromArray(BYTE_SPECIES_128, d, i).reinterpretAsLongs();
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
        // tail as bytes
        for (; i < d.length; i++) {
            subRet0 += Integer.bitCount((q[i] & d[i]) & 0xFF);
            subRet1 += Integer.bitCount((q[i + d.length] & d[i]) & 0xFF);
            subRet2 += Integer.bitCount((q[i + 2 * d.length] & d[i]) & 0xFF);
            subRet3 += Integer.bitCount((q[i + 3 * d.length] & d[i]) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    public static long ipByteBin128(byte[] q, byte[] d) {
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int i = 0;

        var sum0 = IntVector.zero(IntVector.SPECIES_128);
        var sum1 = IntVector.zero(IntVector.SPECIES_128);
        var sum2 = IntVector.zero(IntVector.SPECIES_128);
        var sum3 = IntVector.zero(IntVector.SPECIES_128);
        int limit = ByteVector.SPECIES_128.loopBound(d.length);
        for (; i < limit; i += ByteVector.SPECIES_128.length()) {
            var vd = ByteVector.fromArray(BYTE_SPECIES_128, d, i).reinterpretAsInts();
            var vq0 = ByteVector.fromArray(BYTE_SPECIES_128, q, i).reinterpretAsInts();
            var vq1 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length).reinterpretAsInts();
            var vq2 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length * 2).reinterpretAsInts();
            var vq3 = ByteVector.fromArray(BYTE_SPECIES_128, q, i + d.length * 3).reinterpretAsInts();
            sum0 = sum0.add(vd.and(vq0).lanewise(VectorOperators.BIT_COUNT));
            sum1 = sum1.add(vd.and(vq1).lanewise(VectorOperators.BIT_COUNT));
            sum2 = sum2.add(vd.and(vq2).lanewise(VectorOperators.BIT_COUNT));
            sum3 = sum3.add(vd.and(vq3).lanewise(VectorOperators.BIT_COUNT));
        }
        subRet0 += sum0.reduceLanes(VectorOperators.ADD);
        subRet1 += sum1.reduceLanes(VectorOperators.ADD);
        subRet2 += sum2.reduceLanes(VectorOperators.ADD);
        subRet3 += sum3.reduceLanes(VectorOperators.ADD);
        // tail as bytes
        for (; i < d.length; i++) {
            int dValue = d[i];
            subRet0 += Integer.bitCount((dValue & q[i]) & 0xFF);
            subRet1 += Integer.bitCount((dValue & q[i + d.length]) & 0xFF);
            subRet2 += Integer.bitCount((dValue & q[i + 2 * d.length]) & 0xFF);
            subRet3 += Integer.bitCount((dValue & q[i + 3 * d.length]) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;
    private static final VectorSpecies<Byte> BYTE_SPECIES_FOR_INT_512 = VectorSpecies.of(
        byte.class,
        VectorShape.forBitSize(INT_SPECIES_512.vectorBitSize() / Integer.BYTES)
    );
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Byte> BYTE_SPECIES_FOR_INT_256 = VectorSpecies.of(
        byte.class,
        VectorShape.forBitSize(INT_SPECIES_256.vectorBitSize() / Integer.BYTES)
    );

    static int ipByteBit512(byte[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        IntVector acc = IntVector.zero(INT_SPECIES_512);

        int i = 0;
        for (; i < BYTE_SPECIES_FOR_INT_512.loopBound(q.length); i += BYTE_SPECIES_FOR_INT_512.length()) {
            Vector<Integer> bytes = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_512, q, i).castShape(INT_SPECIES_512, 0);
            long maskBits = Integer.reverse((short) BitUtil.VH_BE_SHORT.get(d, i / 8)) >> 16;

            acc = acc.add(bytes, VectorMask.fromLong(INT_SPECIES_512, maskBits));
        }

        int sum = acc.reduceLanes(VectorOperators.ADD);
        if (i < q.length) {
            // do the tail
            sum += DefaultESVectorUtilSupport.ipByteBitImpl(q, d, i);
        }
        return sum;
    }

    static int ipByteBit256(byte[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        IntVector acc = IntVector.zero(INT_SPECIES_256);

        int i = 0;
        for (; i < BYTE_SPECIES_FOR_INT_256.loopBound(q.length); i += BYTE_SPECIES_FOR_INT_256.length()) {
            Vector<Integer> bytes = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i).castShape(INT_SPECIES_256, 0);
            long maskBits = Integer.reverse(d[i / 8]) >> 24;

            acc = acc.add(bytes, VectorMask.fromLong(INT_SPECIES_256, maskBits));
        }

        int sum = acc.reduceLanes(VectorOperators.ADD);
        if (i < q.length) {
            // do the tail
            sum += DefaultESVectorUtilSupport.ipByteBitImpl(q, d, i);
        }
        return sum;
    }

    private static final VectorSpecies<Float> FLOAT_SPECIES_512 = FloatVector.SPECIES_512;
    private static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;

    static float ipFloatBit512(float[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        FloatVector acc = FloatVector.zero(FLOAT_SPECIES_512);

        int i = 0;
        for (; i < FLOAT_SPECIES_512.loopBound(q.length); i += FLOAT_SPECIES_512.length()) {
            FloatVector floats = FloatVector.fromArray(FLOAT_SPECIES_512, q, i);
            // use the two bytes corresponding to the same sections
            // of the bit vector as a mask for addition
            long maskBits = Integer.reverse((short) BitUtil.VH_BE_SHORT.get(d, i / 8)) >> 16;
            acc = acc.add(floats, VectorMask.fromLong(FLOAT_SPECIES_512, maskBits));
        }

        float sum = acc.reduceLanes(VectorOperators.ADD);
        if (i < q.length) {
            // do the tail
            sum += DefaultESVectorUtilSupport.ipFloatBitImpl(q, d, i);
        }

        return sum;
    }

    static float ipFloatBit256(float[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        FloatVector acc = FloatVector.zero(FLOAT_SPECIES_256);

        int i = 0;
        for (; i < FLOAT_SPECIES_256.loopBound(q.length); i += FLOAT_SPECIES_256.length()) {
            FloatVector floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);
            // use the byte corresponding to the same section
            // of the bit vector as a mask for addition
            long maskBits = Integer.reverse(d[i / 8]) >> 24;
            acc = acc.add(floats, VectorMask.fromLong(FLOAT_SPECIES_256, maskBits));
        }

        float sum = acc.reduceLanes(VectorOperators.ADD);
        if (i < q.length) {
            // do the tail
            sum += DefaultESVectorUtilSupport.ipFloatBitImpl(q, d, i);
        }

        return sum;
    }
}
