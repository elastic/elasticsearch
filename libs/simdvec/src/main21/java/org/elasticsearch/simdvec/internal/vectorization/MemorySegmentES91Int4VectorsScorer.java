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
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.S2I;

/** Panamized scorer for quantized vectors stored as an {@link IndexInput}.
 * <p>
 * Similar to {@link org.apache.lucene.util.VectorUtil#int4DotProduct(byte[], byte[])} but
 *  one value is read directly from a {@link MemorySegment}.
 * */
public final class MemorySegmentES91Int4VectorsScorer extends ES91Int4VectorsScorer {

    private static final VectorSpecies<Byte> BYTE_SPECIES_64 = ByteVector.SPECIES_64;
    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;

    private static final VectorSpecies<Short> SHORT_SPECIES_128 = ShortVector.SPECIES_128;
    private static final VectorSpecies<Short> SHORT_SPECIES_256 = ShortVector.SPECIES_256;

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;

    private final MemorySegment memorySegment;

    public MemorySegmentES91Int4VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions);
        this.memorySegment = memorySegment;
    }

    @Override
    public long int4DotProduct(byte[] q) throws IOException {
        if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 512 || PanamaESVectorUtilSupport.VECTOR_BITSIZE == 256) {
            return dotProduct(q);
        }
        int i = 0;
        int res = 0;
        if (dimensions >= 32 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            i += BYTE_SPECIES_128.loopBound(dimensions);
            res += int4DotProductBody128(q, i);
        }

        while (i < dimensions) {
            res += in.readByte() * q[i++];
        }
        return res;
    }

    private int int4DotProductBody128(byte[] q, int limit) throws IOException {
        int sum = 0;
        long offset = in.getFilePointer();
        for (int i = 0; i < limit; i += 1024) {
            ShortVector acc0 = ShortVector.zero(SHORT_SPECIES_128);
            ShortVector acc1 = ShortVector.zero(SHORT_SPECIES_128);
            int innerLimit = Math.min(limit - i, 1024);
            for (int j = 0; j < innerLimit; j += BYTE_SPECIES_128.length()) {
                ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i + j);
                ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i + j, LITTLE_ENDIAN);
                ByteVector prod8 = va8.mul(vb8);
                ShortVector prod16 = prod8.convertShape(B2S, ShortVector.SPECIES_128, 0).reinterpretAsShorts();
                acc0 = acc0.add(prod16.and((short) 255));
                va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i + j + 8);
                vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i + j + 8, LITTLE_ENDIAN);
                prod8 = va8.mul(vb8);
                prod16 = prod8.convertShape(B2S, SHORT_SPECIES_128, 0).reinterpretAsShorts();
                acc1 = acc1.add(prod16.and((short) 255));
            }

            IntVector intAcc0 = acc0.convertShape(S2I, INT_SPECIES_128, 0).reinterpretAsInts();
            IntVector intAcc1 = acc0.convertShape(S2I, INT_SPECIES_128, 1).reinterpretAsInts();
            IntVector intAcc2 = acc1.convertShape(S2I, INT_SPECIES_128, 0).reinterpretAsInts();
            IntVector intAcc3 = acc1.convertShape(S2I, INT_SPECIES_128, 1).reinterpretAsInts();
            sum += intAcc0.add(intAcc1).add(intAcc2).add(intAcc3).reduceLanes(ADD);
        }
        in.seek(offset + limit);
        return sum;
    }

    private long dotProduct(byte[] q) throws IOException {
        int i = 0;
        int res = 0;

        // only vectorize if we'll at least enter the loop a single time, and we have at least 128-bit
        // vectors (256-bit on intel to dodge performance landmines)
        if (dimensions >= 16 && PanamaESVectorUtilSupport.HAS_FAST_INTEGER_VECTORS) {
            // compute vectorized dot product consistent with VPDPBUSD instruction
            if (PanamaESVectorUtilSupport.VECTOR_BITSIZE >= 512) {
                i += BYTE_SPECIES_128.loopBound(dimensions);
                res += dotProductBody512(q, i);
            } else if (PanamaESVectorUtilSupport.VECTOR_BITSIZE == 256) {
                i += BYTE_SPECIES_64.loopBound(dimensions);
                res += dotProductBody256(q, i);
            } else {
                // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
                i += BYTE_SPECIES_64.loopBound(dimensions - BYTE_SPECIES_64.length());
                res += dotProductBody128(q, i);
            }
        }
        // scalar tail
        for (; i < q.length; i++) {
            res += in.readByte() * q[i];
        }
        return res;
    }

    /** vectorized dot product body (512 bit vectors) */
    private int dotProductBody512(byte[] q, int limit) throws IOException {
        IntVector acc = IntVector.zero(INT_SPECIES_512);
        long offset = in.getFilePointer();
        for (int i = 0; i < limit; i += BYTE_SPECIES_128.length()) {
            ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_128, q, i);
            ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_128, memorySegment, offset + i, LITTLE_ENDIAN);

            // 16-bit multiply: avoid AVX-512 heavy multiply on zmm
            Vector<Short> va16 = va8.convertShape(B2S, SHORT_SPECIES_256, 0);
            Vector<Short> vb16 = vb8.convertShape(B2S, SHORT_SPECIES_256, 0);
            Vector<Short> prod16 = va16.mul(vb16);

            // 32-bit add
            Vector<Integer> prod32 = prod16.convertShape(S2I, INT_SPECIES_512, 0);
            acc = acc.add(prod32);
        }

        in.seek(offset + limit); // advance the input stream
        // reduce
        return acc.reduceLanes(ADD);
    }

    /** vectorized dot product body (256 bit vectors) */
    private int dotProductBody256(byte[] q, int limit) throws IOException {
        IntVector acc = IntVector.zero(INT_SPECIES_256);
        long offset = in.getFilePointer();
        for (int i = 0; i < limit; i += BYTE_SPECIES_64.length()) {
            ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
            ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

            // 32-bit multiply and add into accumulator
            Vector<Integer> va32 = va8.convertShape(B2I, INT_SPECIES_256, 0);
            Vector<Integer> vb32 = vb8.convertShape(B2I, INT_SPECIES_256, 0);
            acc = acc.add(va32.mul(vb32));
        }
        in.seek(offset + limit);
        // reduce
        return acc.reduceLanes(ADD);
    }

    /** vectorized dot product body (128 bit vectors) */
    private int dotProductBody128(byte[] q, int limit) throws IOException {
        IntVector acc = IntVector.zero(INT_SPECIES_128);
        long offset = in.getFilePointer();
        // 4 bytes at a time (re-loading half the vector each time!)
        for (int i = 0; i < limit; i += BYTE_SPECIES_64.length() >> 1) {
            // load 8 bytes
            ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
            ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

            // process first "half" only: 16-bit multiply
            Vector<Short> va16 = va8.convert(B2S, 0);
            Vector<Short> vb16 = vb8.convert(B2S, 0);
            Vector<Short> prod16 = va16.mul(vb16);

            // 32-bit add
            acc = acc.add(prod16.convertShape(S2I, INT_SPECIES_128, 0));
        }
        in.seek(offset + limit);
        // reduce
        return acc.reduceLanes(ADD);
    }
}
