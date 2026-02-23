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
import java.lang.foreign.ValueLayout;
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

    protected MemorySegmentES92PanamaInt7VectorsScorer(IndexInput in, int dimensions, int bulkSize) {
        super(in, dimensions, bulkSize);
        IndexInputUtils.checkInputType(in);
    }

    protected long panamaInt7DotProduct(byte[] q) throws IOException {
        assert dimensions == q.length;
        return IndexInputUtils.withSlice(in, dimensions, segment -> panamaInt7DotProductImpl(q, segment, dimensions));
    }

    private static long panamaInt7DotProductImpl(byte[] q, MemorySegment segment, int dimensions) {
        int i = 0;
        int res = 0;
        // only vectorize if we'll at least enter the loop a single time
        if (dimensions >= 16) {
            // compute vectorized dot product consistent with VPDPBUSD instruction
            if (VECTOR_BITSIZE >= 512) {
                int limit = BYTE_SPECIES_128.loopBound(dimensions);
                res += dotProductBody512Impl(q, segment, 0, limit);
                i = limit;
            } else if (VECTOR_BITSIZE == 256) {
                int limit = BYTE_SPECIES_64.loopBound(dimensions);
                res += dotProductBody256Impl(q, segment, 0, limit);
                i = limit;
            } else {
                // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
                int limit = BYTE_SPECIES_64.loopBound(dimensions - BYTE_SPECIES_64.length());
                res += dotProductBody128Impl(q, segment, 0, limit);
                i = limit;
            }
        }
        // scalar tail
        for (; i < dimensions; i++) {
            res += segment.get(ValueLayout.JAVA_BYTE, i) * q[i];
        }
        return res;
    }

    private static int dotProductBody512Impl(byte[] q, MemorySegment memorySegment, long offset, int limit) {
        IntVector acc = IntVector.zero(INT_SPECIES_512);
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
        return acc.reduceLanes(ADD);
    }

    private static int dotProductBody256Impl(byte[] q, MemorySegment memorySegment, long offset, int limit) {
        IntVector acc = IntVector.zero(INT_SPECIES_256);
        for (int i = 0; i < limit; i += BYTE_SPECIES_64.length()) {
            ByteVector va8 = ByteVector.fromArray(BYTE_SPECIES_64, q, i);
            ByteVector vb8 = ByteVector.fromMemorySegment(BYTE_SPECIES_64, memorySegment, offset + i, LITTLE_ENDIAN);

            // 32-bit multiply and add into accumulator
            Vector<Integer> va32 = va8.convertShape(B2I, INT_SPECIES_256, 0);
            Vector<Integer> vb32 = vb8.convertShape(B2I, INT_SPECIES_256, 0);
            acc = acc.add(va32.mul(vb32));
        }
        return acc.reduceLanes(ADD);
    }

    private static int dotProductBody128Impl(byte[] q, MemorySegment memorySegment, long offset, int limit) {
        IntVector acc = IntVector.zero(IntVector.SPECIES_128);
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
        return acc.reduceLanes(ADD);
    }

    protected void panamaInt7DotProductBulk(byte[] q, int count, float[] scores) throws IOException {
        assert dimensions == q.length;
        IndexInputUtils.withSlice(in, (long) dimensions * count, segment -> {
            panamaInt7DotProductBulkImpl(q, segment, dimensions, count, scores);
            return null;
        });
    }

    private static void panamaInt7DotProductBulkImpl(byte[] q, MemorySegment memorySegment, int dimensions, int count, float[] scores) {
        // only vectorize if we'll at least enter the loop a single time
        if (dimensions >= 16) {
            // compute vectorized dot product consistent with VPDPBUSD instruction
            if (VECTOR_BITSIZE >= 512) {
                dotProductBulkVectorized512(q, memorySegment, dimensions, count, scores);
            } else if (VECTOR_BITSIZE == 256) {
                dotProductBulkVectorized256(q, memorySegment, dimensions, count, scores);
            } else {
                // tricky: we don't have SPECIES_32, so we workaround with "overlapping read"
                dotProductBulkVectorized128(q, memorySegment, dimensions, count, scores);
            }
        } else {
            for (int iter = 0; iter < count; iter++) {
                long base = (long) iter * dimensions;
                long res = 0;
                for (int i = 0; i < dimensions; i++) {
                    res += memorySegment.get(ValueLayout.JAVA_BYTE, base + i) * q[i];
                }
                scores[iter] = res;
            }
        }
    }

    private static void dotProductBulkVectorized512(byte[] q, MemorySegment memorySegment, int dimensions, int count, float[] scores) {
        int limit = BYTE_SPECIES_128.loopBound(dimensions);
        for (int iter = 0; iter < count; iter++) {
            long base = (long) iter * dimensions;
            long res = dotProductBody512Impl(q, memorySegment, base, limit);
            for (int i = limit; i < dimensions; i++) {
                res += memorySegment.get(ValueLayout.JAVA_BYTE, base + i) * q[i];
            }
            scores[iter] = res;
        }
    }

    private static void dotProductBulkVectorized256(byte[] q, MemorySegment memorySegment, int dimensions, int count, float[] scores) {
        int limit = BYTE_SPECIES_128.loopBound(dimensions);
        for (int iter = 0; iter < count; iter++) {
            long base = (long) iter * dimensions;
            long res = dotProductBody256Impl(q, memorySegment, base, limit);
            for (int i = limit; i < dimensions; i++) {
                res += memorySegment.get(ValueLayout.JAVA_BYTE, base + i) * q[i];
            }
            scores[iter] = res;
        }
    }

    private static void dotProductBulkVectorized128(byte[] q, MemorySegment memorySegment, int dimensions, int count, float[] scores) {
        int limit = BYTE_SPECIES_64.loopBound(dimensions - BYTE_SPECIES_64.length());
        for (int iter = 0; iter < count; iter++) {
            long base = (long) iter * dimensions;
            long res = dotProductBody128Impl(q, memorySegment, base, limit);
            for (int i = limit; i < dimensions; i++) {
                res += memorySegment.get(ValueLayout.JAVA_BYTE, base + i) * q[i];
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
        float[] scores,
        int bulkSize
    ) throws IOException {
        IndexInputUtils.withSlice(in, 16L * bulkSize, memorySegment -> {
            applyCorrectionsBulkImpl(
                memorySegment,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                scores,
                bulkSize,
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                dimensions
            );
            return null;
        });
    }

    private static void applyCorrectionsBulkImpl(
        MemorySegment memorySegment,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores,
        int bulkSize,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        int dimensions
    ) {
        int limit = FLOAT_SPECIES.loopBound(bulkSize);
        int i = 0;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * SEVEN_BIT_SCALE;
        float y1 = queryComponentSum;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, i * Float.BYTES, ByteOrder.LITTLE_ENDIAN);
            var lx = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, 4 * bulkSize + i * Float.BYTES, ByteOrder.LITTLE_ENDIAN)
                .sub(ax)
                .mul(SEVEN_BIT_SCALE);
            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES,
                memorySegment,
                8 * bulkSize + i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN
            ).convert(VectorOperators.I2F, 0);
            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                12 * bulkSize + i * Float.BYTES,
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
        // process tail: do one masked vector iteration
        if (i < bulkSize) {
            var floatVectorMask = FLOAT_SPECIES.indexInRange(i, bulkSize);
            var intVectorMask = INT_SPECIES.indexInRange(i, bulkSize);

            var ax = FloatVector.fromMemorySegment(FLOAT_SPECIES, memorySegment, (long) i * Float.BYTES, LITTLE_ENDIAN, floatVectorMask);
            var upper = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                4L * bulkSize + (long) i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                floatVectorMask
            );
            var lx = upper.sub(ax).mul(SEVEN_BIT_SCALE);

            var targetComponentSums = IntVector.fromMemorySegment(
                INT_SPECIES,
                memorySegment,
                8L * bulkSize + (long) i * Integer.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                intVectorMask
            ).convert(VectorOperators.I2F, 0);

            var additionalCorrections = FloatVector.fromMemorySegment(
                FLOAT_SPECIES,
                memorySegment,
                12L * bulkSize + (long) i * Float.BYTES,
                ByteOrder.LITTLE_ENDIAN,
                floatVectorMask
            );

            var qcDist = FloatVector.fromArray(FLOAT_SPECIES, scores, i, floatVectorMask);

            var res1 = ax.mul(ay).mul(dimensions);
            var res2 = lx.mul(ay).mul(targetComponentSums);
            var res3 = ax.mul(ly).mul(y1);
            var res4 = lx.mul(ly).mul(qcDist);
            var res = res1.add(res2).add(res3).add(res4);

            if (similarityFunction == EUCLIDEAN) {
                res = res.mul(-2).add(additionalCorrections).add(queryAdditionalCorrection).add(1f);
                res = FloatVector.broadcast(FLOAT_SPECIES, 1).div(res).max(0);
                res.intoArray(scores, i, floatVectorMask);
            } else {
                res = res.add(queryAdditionalCorrection).add(additionalCorrections).sub(centroidDp);
                if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                    res.intoArray(scores, i, floatVectorMask);
                    for (int j = 0, n = bulkSize - i; j < n; j++) {
                        scores[i + j] = VectorUtil.scaleMaxInnerProductScore(scores[i + j]);
                    }
                } else {
                    res = res.add(1f).mul(0.5f).max(0);
                    res.intoArray(scores, i, floatVectorMask);
                }
            }
        }
    }

}
