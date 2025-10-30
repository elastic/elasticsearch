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
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.B2I;
import static jdk.incubator.vector.VectorOperators.B2S;
import static jdk.incubator.vector.VectorOperators.S2I;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Panamized scorer for 7-bit quantized vectors stored as an {@link IndexInput}. **/
abstract class MemorySegmentES92PanamaInt7VectorsScorer extends ES92Int7VectorsScorer {

    private static final VectorSpecies<Byte> BYTE_SPECIES_64 = ByteVector.SPECIES_64;
    private static final VectorSpecies<Byte> BYTE_SPECIES_128 = ByteVector.SPECIES_128;

    private static final VectorSpecies<Short> SHORT_SPECIES_128 = ShortVector.SPECIES_128;
    private static final VectorSpecies<Short> SHORT_SPECIES_256 = ShortVector.SPECIES_256;

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final VectorSpecies<Integer> INT_SPECIES_256 = IntVector.SPECIES_256;
    private static final VectorSpecies<Integer> INT_SPECIES_512 = IntVector.SPECIES_512;

    private static final int VECTOR_BITSIZE;
    private static final VectorSpecies<Float> FLOAT_SPECIES;
    private static final VectorSpecies<Integer> INT_SPECIES;

    static {
        // default to platform supported bitsize
        VECTOR_BITSIZE = VectorShape.preferredShape().vectorBitSize();
        FLOAT_SPECIES = VectorSpecies.of(float.class, VectorShape.forBitSize(VECTOR_BITSIZE));
        INT_SPECIES = VectorSpecies.of(int.class, VectorShape.forBitSize(VECTOR_BITSIZE));
    }

    protected final MemorySegment memorySegment;

    protected MemorySegmentES92PanamaInt7VectorsScorer(IndexInput in, int dimensions, MemorySegment memorySegment) {
        super(in, dimensions);
        this.memorySegment = memorySegment;
    }

    protected long panamaInt7DotProduct(byte[] q) throws IOException {
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

    protected void panamaInt7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
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
            super.int7DotProductBulk(q, count, scores);
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

    protected void applyCorrectionsBulk(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        int limit = FLOAT_SPECIES.loopBound(BULK_SIZE);
        int i = 0;
        long offset = in.getFilePointer();
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * SEVEN_BIT_SCALE;
        float y1 = queryComponentSum;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, offset + i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                offset + 4 * BULK_SIZE + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).sub(ax).mul(SEVEN_BIT_SCALE);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES,
                memorySegment,
                offset + 8 * BULK_SIZE + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                offset + 12 * BULK_SIZE + i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN
            );
            var qcDist = FloatVector.fromArray(FLOAT_SPECIES, scores, i);
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
                res = FloatVector.broadcast(FLOAT_SPECIES, 1).div(res).max(0);
                res.intoArray(scores, i);
            } else {
                // For cosine and max inner product, we need to apply the additional correction, which is
                // assumed to be the non-centered dot-product between the vector and the centroid
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i);
                    // not sure how to do it better
                    for (int j = 0; j < FLOAT_SPECIES.length(); j++) {
                        scores[i + j] = VectorUtil.scaleMaxInnerProductScore(scores[i + j]);
                    }
                } else {
                    res = res.add(1f).mul(0.5f).max(0);
                    res.intoArray(scores, i);
                }
            }
        }
        in.seek(offset + 16L * BULK_SIZE);
    }
}
