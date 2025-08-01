/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.S2I;

/** Panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
abstract class MemorySegmentES92FallBackInt7VectorsScorer extends ES92Int7VectorsScorer {

    private static final VectorSpecies<Byte> BYTE_SPECIES_64 = ByteVector.SPECIES_64;
    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;

    private static final VectorSpecies<Short> SHORT_SPECIES_128 = ShortVector.SPECIES_128;
    private static final VectorSpecies<Short> SHORT_SPECIES_256 = ShortVector.SPECIES_256;

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;

    private static final int VECTOR_BITSIZE;
    protected static final VectorSpecies<Float> FLOAT_SPECIES;
    protected static final VectorSpecies<Integer> INT_SPECIES;

    static {
        // default to platform supported bitsize
        VECTOR_BITSIZE = VectorShape.preferredShape().vectorBitSize();
        FLOAT_SPECIES = VectorSpecies.of(float.class, VectorShape.forBitSize(VECTOR_BITSIZE));
        INT_SPECIES = VectorSpecies.of(int.class, VectorShape.forBitSize(VECTOR_BITSIZE));
    }

    protected final MemorySegment memorySegment;

    public MemorySegmentES92FallBackInt7VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions);
        this.memorySegment = memorySegment;
    }

    protected long fallbackInt7DotProduct(byte[] q) throws IOException {
        assert dimensions == q.length;
        int i = 0;
        int res = 0;
        // only vectorize if we'll at least enter the loop a single time
        if (dimensions >= 16) {
            // compute vectorized dot product consistent with VPDPBUSD instruction
            if (VECTOR_BITSIZE >= 512) {
                i += BYTE_SPECIES_128.loopBound(dimensions);
                res += dotProductBody512(q, i);
            } else if (VECTOR_BITSIZE == 256) {
                i += BYTE_SPECIES_64.loopBound(dimensions);
                res += dotProductBody256(q, i);
            } else {
                // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
                i += BYTE_SPECIES_64.loopBound(dimensions - BYTE_SPECIES_64.length());
                res += dotProductBody128(q, i);
            }
            // scalar tail
            while (i < dimensions) {
                res += in.readByte() * q[i++];
            }
            return res;
        } else {
            return super.int7DotProduct(q);
        }
    }

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

    private int dotProductBody128(byte[] q, int limit) throws IOException {
        IntVector acc = IntVector.zero(IntVector.SPECIES_128);
        long offset = in.getFilePointer();
        // 4 bytes at a time (re-loading half the vector each time!)
        for (int i = 0; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
            // load 8 bytes
            ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
            ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

            // process first "half" only: 16-bit multiply
            Vector<Short> va16 = va8.convert(B2S, 0);
            Vector<Short> vb16 = vb8.convert(B2S, 0);
            Vector<Short> prod16 = va16.mul(vb16);

            // 32-bit add
            acc = acc.add(prod16.convertShape(S2I, IntVector.SPECIES_128, 0));
        }
        in.seek(offset + limit);
        // reduce
        return acc.reduceLanes(ADD);
    }

    protected void fallbackInt7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert dimensions == q.length;
        // only vectorize if we'll at least enter the loop a single time
        if (dimensions >= 16) {
            // compute vectorized dot product consistent with VPDPBUSD instruction
            if (VECTOR_BITSIZE >= 512) {
                dotProductBody512Bulk(q, count, scores);
            } else if (VECTOR_BITSIZE == 256) {
                dotProductBody256Bulk(q, count, scores);
            } else {
                // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
                dotProductBody128Bulk(q, count, scores);
            }
        } else {
            int7DotProductBulk(q, count, scores);
        }
    }

    private void dotProductBody512Bulk(byte[] q, int count, float[] scores) throws IOException {
        int limit = BYTE_SPECIES_128.loopBound(dimensions);
        for (int iter = 0; iter < count; iter++) {
            IntVector acc = IntVector.zero(INT_SPECIES_512);
            long offset = in.getFilePointer();
            int i = 0;
            for (; i < limit; i += BYTE_SPECIES_128.length()) {
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
            long res = acc.reduceLanes(ADD);
            for (; i < dimensions; i++) {
                res += in.readByte() * q[i];
            }
            scores[iter] = res;
        }
    }

    private void dotProductBody256Bulk(byte[] q, int count, float[] scores) throws IOException {
        int limit = BYTE_SPECIES_128.loopBound(dimensions);
        for (int iter = 0; iter < count; iter++) {
            IntVector acc = IntVector.zero(INT_SPECIES_256);
            long offset = in.getFilePointer();
            int i = 0;
            for (; i < limit; i += BYTE_SPECIES_64.length()) {
                ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
                ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

                // 32-bit multiply and add into accumulator
                Vector<Integer> va32 = va8.convertShape(B2I, INT_SPECIES_256, 0);
                Vector<Integer> vb32 = vb8.convertShape(B2I, INT_SPECIES_256, 0);
                acc = acc.add(va32.mul(vb32));
            }
            in.seek(offset + limit);
            // reduce
            long res = acc.reduceLanes(ADD);
            for (; i < dimensions; i++) {
                res += in.readByte() * q[i];
            }
            scores[iter] = res;
        }
    }

    private void dotProductBody128Bulk(byte[] q, int count, float[] scores) throws IOException {
        int limit = BYTE_SPECIES_64.loopBound(dimensions - BYTE_SPECIES_64.length());
        for (int iter = 0; iter < count; iter++) {
            IntVector acc = IntVector.zero(IntVector.SPECIES_128);
            long offset = in.getFilePointer();
            // 4 bytes at a time (re-loading half the vector each time!)
            int i = 0;
            for (; i < limit; i += ByteVector.SPECIES_64.length() >> 1) {
                // load 8 bytes
                ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
                ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

                // process first "half" only: 16-bit multiply
                Vector<Short> va16 = va8.convert(B2S, 0);
                Vector<Short> vb16 = vb8.convert(B2S, 0);
                Vector<Short> prod16 = va16.mul(vb16);

                // 32-bit add
                acc = acc.add(prod16.convertShape(S2I, IntVector.SPECIES_128, 0));
            }
            in.seek(offset + limit);
            // reduce
            long res = acc.reduceLanes(ADD);
            for (; i < dimensions; i++) {
                res += in.readByte() * q[i];
            }
            scores[iter] = res;
        }
    }
}
