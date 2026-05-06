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
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.MathUtils;
import org.elasticsearch.simdvec.MultiBFloat16VectorsSource;
import org.elasticsearch.simdvec.MultiByteVectorsSource;
import org.elasticsearch.simdvec.MultiFloatVectorsSource;
import org.elasticsearch.simdvec.MultiVectorsSource;
import org.elasticsearch.simdvec.internal.Similarities;

import java.lang.foreign.MemorySegment;
import java.nio.ShortBuffer;

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.AND;
import static jdk.incubator.vector.VectorOperators.ASHR;
import static jdk.incubator.vector.VectorOperators.LSHL;
import static jdk.incubator.vector.VectorOperators.LSHR;
import static jdk.incubator.vector.VectorOperators.MAX;
import static jdk.incubator.vector.VectorOperators.MIN;
import static jdk.incubator.vector.VectorOperators.OR;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public final class PanamaESVectorUtilSupport implements ESVectorUtilSupport {

    static final int VECTOR_BITSIZE = PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE;
    private static final DefaultESVectorUtilSupport DEFAULT = new DefaultESVectorUtilSupport();

    private static final VectorSpecies<Float> FLOAT_SPECIES = PanamaVectorConstants.PREFERRED_FLOAT_SPECIES;
    private static final VectorSpecies<Integer> INTEGER_SPECIES = PanamaVectorConstants.PREFERRED_INTEGER_SPECIES;
    /** Whether integer vectors can be trusted to actually be fast. */
    static final boolean HAS_FAST_INTEGER_VECTORS = PanamaVectorConstants.ENABLE_INTEGER_VECTORS;

    static final boolean SUPPORTS_NATIVE_VECTORS = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    private static FloatVector fma(FloatVector a, FloatVector b, FloatVector c) {
        if (Constants.HAS_FAST_VECTOR_FMA) {
            return a.fma(b, c);
        } else {
            return a.mul(b).add(c);
        }
    }

    private static float fma(float a, float b, float c) {
        if (Constants.HAS_FAST_SCALAR_FMA) {
            return Math.fma(a, b, c);
        } else {
            return a * b + c;
        }
    }

    // BFloats as shorts needs to be half the float vector bitsize
    private static final VectorSpecies<Short> BFLOAT_SPECIES;

    static {
        VectorSpecies<Short> species;
        try {
            species = VectorSpecies.of(short.class, VectorShape.forBitSize(FLOAT_SPECIES.vectorBitSize() / 2));
        } catch (IllegalArgumentException e) {
            species = null;
        }
        BFLOAT_SPECIES = species;
    }

    @Override
    public void floatToBFloat16(float[] floats, ShortBuffer bFloats) {
        if (!SUPPORTS_HEAP_SEGMENTS || BFLOAT_SPECIES == null) {
            DefaultESVectorUtilSupport.floatToBFloat16(floats, bFloats, 0);
        } else {
            MemorySegment buffer = MemorySegment.ofBuffer(bFloats);
            int vectorEnd = FLOAT_SPECIES.loopBound(floats.length);

            for (int i = 0; i < vectorEnd; i += FLOAT_SPECIES.length()) {
                IntVector bits = FloatVector.fromArray(FLOAT_SPECIES, floats, i).reinterpretAsInts();
                // roundingBias = 0x7fff + ((bits >> 16) & 1)
                IntVector bias = bits.lanewise(LSHR, 16).lanewise(AND, 1).add(0x7fff);
                bits = bits.add(bias);
                bits.lanewise(LSHR, 16)
                    .convertShape(VectorOperators.I2S, BFLOAT_SPECIES, 0)
                    .intoMemorySegment(buffer, (long) i * Short.BYTES, bFloats.order());
            }
            bFloats.position(bFloats.position() + vectorEnd);

            // scalar tail
            DefaultESVectorUtilSupport.floatToBFloat16(floats, bFloats, vectorEnd);
        }
    }

    @Override
    public void bFloat16ToFloat(ShortBuffer bFloats, float[] floats) {
        if (!SUPPORTS_HEAP_SEGMENTS || BFLOAT_SPECIES == null) {
            DefaultESVectorUtilSupport.bFloat16ToFloat(bFloats, floats, 0);
        } else {
            MemorySegment buffer = MemorySegment.ofBuffer(bFloats);
            int vectorEnd = BFLOAT_SPECIES.loopBound(floats.length);    // take the number of elements as the length of the array

            for (int i = 0; i < vectorEnd; i += BFLOAT_SPECIES.length()) {
                ShortVector sv = ShortVector.fromMemorySegment(BFLOAT_SPECIES, buffer, (long) i * Short.BYTES, bFloats.order());
                sv.convertShape(VectorOperators.ZERO_EXTEND_S2I, INTEGER_SPECIES, 0)
                    .lanewise(LSHL, 16)
                    .reinterpretAsFloats()
                    .intoArray(floats, i);
            }
            bFloats.position(bFloats.position() + vectorEnd);

            // scalar tail
            DefaultESVectorUtilSupport.bFloat16ToFloat(bFloats, floats, vectorEnd);
        }
    }

    @Override
    public float dotProduct(float[] a, float[] b) {
        return SUPPORTS_NATIVE_VECTORS && SUPPORTS_HEAP_SEGMENTS
            ? Similarities.dotProductF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length)
            : VectorUtil.dotProduct(a, b);
    }

    @Override
    public float squareDistance(float[] a, float[] b) {
        return SUPPORTS_NATIVE_VECTORS && SUPPORTS_HEAP_SEGMENTS
            ? Similarities.squareDistanceF32(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length)
            : VectorUtil.squareDistance(a, b);
    }

    @Override
    public float squareDistance(float[] a, float[] b, int offset, int length) {
        if (offset == 0 && length == a.length) {
            return squareDistance(a, b);
        }
        FloatVector acc = FloatVector.zero(FLOAT_SPECIES);
        int i = offset;
        final int end = offset + length;
        final int vectorEnd = offset + FLOAT_SPECIES.loopBound(length);
        for (; i < vectorEnd; i += FLOAT_SPECIES.length()) {
            FloatVector av = FloatVector.fromArray(FLOAT_SPECIES, a, i);
            FloatVector bv = FloatVector.fromArray(FLOAT_SPECIES, b, i);
            FloatVector diff = av.sub(bv);
            acc = fma(diff, diff, acc);
        }
        float distance = acc.reduceLanes(ADD);
        for (; i < end; i++) {
            float diff = a[i] - b[i];
            distance = fma(diff, diff, distance);
        }
        return distance;
    }

    @Override
    public float cosine(byte[] a, byte[] b) {
        return SUPPORTS_NATIVE_VECTORS && SUPPORTS_HEAP_SEGMENTS
            ? Similarities.cosineI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length)
            : VectorUtil.cosine(a, b);
    }

    @Override
    public float dotProduct(byte[] a, byte[] b) {
        return SUPPORTS_NATIVE_VECTORS && SUPPORTS_HEAP_SEGMENTS
            ? Similarities.dotProductI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length)
            : VectorUtil.dotProduct(a, b);
    }

    @Override
    public float maxSimDotProduct(MultiFloatVectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseF32BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (float[] floats : query) {
                Similarities.dotProductF32Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(floats),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
            }
            return sum;
        }
        return DEFAULT.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float maxSimDotProduct(MultiBFloat16VectorsSource source, float[][] query, float[] scoresScratch) {
        if (canUseBFloat16Path(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (float[] floats : query) {
                Similarities.dotProductDBF16QF32Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(floats),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
            }
            return sum;
        }
        return DEFAULT.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float maxSimDotProduct(MultiByteVectorsSource source, byte[][] query, float[] scoresScratch) {
        if (canUseI8BulkPath(source)) {
            final BytesRef vectors = source.vectorBytes();
            final MemorySegment vectorsSegment = MemorySegment.ofArray(vectors.bytes)
                .asSlice(vectors.offset, (long) source.vectorByteSize() * source.vectorCount());
            final MemorySegment scoresSegment = MemorySegment.ofArray(scoresScratch);
            float sum = 0f;
            for (byte[] bytes : query) {
                Similarities.dotProductI8Bulk(
                    vectorsSegment,
                    MemorySegment.ofArray(bytes),
                    source.vectorDims(),
                    source.vectorCount(),
                    scoresSegment
                );
                sum += ESVectorUtil.max(scoresScratch, source.vectorCount());
            }
            return sum;
        }
        return DEFAULT.maxSimDotProduct(source, query, scoresScratch);
    }

    @Override
    public float squareDistance(byte[] a, byte[] b) {
        return SUPPORTS_NATIVE_VECTORS && SUPPORTS_HEAP_SEGMENTS
            ? Similarities.squareDistanceI8(MemorySegment.ofArray(a), MemorySegment.ofArray(b), a.length)
            : VectorUtil.squareDistance(a, b);
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
        if (BYTE_SPECIES_FOR_PREFFERED_FLOATS != null && q.length >= PREFERRED_FLOAT_SPECIES.length()) {
            return ipFloatByteImpl(q, d);
        }
        return DefaultESVectorUtilSupport.ipFloatByteImpl(q, d);
    }

    @Override
    public void centerAndCalculateOSQStatsEuclidean(float[] vector, float[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length;
        assert vector.length == centered.length;
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        int i = 0;
        int vectCount = 0;
        if (vector.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector vecMeanVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector m2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector norm2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector minVec = FloatVector.broadcast(FLOAT_SPECIES, Float.MAX_VALUE);
            FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, -Float.MAX_VALUE);
            int count = 0;
            for (; i < FLOAT_SPECIES.loopBound(vector.length); i += FLOAT_SPECIES.length()) {
                ++count;
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
                FloatVector c = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                FloatVector centeredVec = v.sub(c);
                FloatVector deltaVec = centeredVec.sub(vecMeanVec);
                norm2Vec = fma(centeredVec, centeredVec, norm2Vec);
                vecMeanVec = vecMeanVec.add(deltaVec.mul(1f / count));
                FloatVector delta2Vec = centeredVec.sub(vecMeanVec);
                m2Vec = fma(deltaVec, delta2Vec, m2Vec);
                minVec = minVec.min(centeredVec);
                maxVec = maxVec.max(centeredVec);
                centeredVec.intoArray(centered, i);
            }
            min = minVec.reduceLanes(MIN);
            max = maxVec.reduceLanes(MAX);
            norm2 = norm2Vec.reduceLanes(ADD);
            vecMean = vecMeanVec.reduceLanes(ADD) / FLOAT_SPECIES.length();
            FloatVector d2Mean = vecMeanVec.sub(vecMean);
            m2Vec = fma(d2Mean, d2Mean, m2Vec);
            vectCount = count * FLOAT_SPECIES.length();
            vecVar = m2Vec.reduceLanes(ADD);
        }

        float tailMean = 0;
        float tailM2 = 0;
        int tailCount = 0;
        // handle the tail
        for (; i < vector.length; i++) {
            centered[i] = vector[i] - centroid[i];
            float delta = centered[i] - tailMean;
            ++tailCount;
            tailMean += delta / tailCount;
            float delta2 = centered[i] - tailMean;
            tailM2 = fma(delta, delta2, tailM2);
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
        }
        if (vectCount == 0) {
            vecMean = tailMean;
            vecVar = tailM2;
        } else if (tailCount > 0) {
            int totalCount = tailCount + vectCount;
            assert totalCount == vector.length;
            float alpha = (float) vectCount / totalCount;
            float beta = 1f - alpha;
            float completeMean = alpha * vecMean + beta * tailMean;
            float dMean2Lhs = (vecMean - completeMean) * (vecMean - completeMean);
            float dMean2Rhs = (tailMean - completeMean) * (tailMean - completeMean);
            vecVar = (vecVar + dMean2Lhs) + beta * (tailM2 + dMean2Rhs);
            vecMean = completeMean;
        }
        stats[0] = vecMean;
        stats[1] = vecVar / vector.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    @Override
    public void centerAndCalculateOSQStatsDp(float[] vector, float[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length;
        assert vector.length == centered.length;
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        float centroidDot = 0;
        int i = 0;
        int vectCount = 0;
        int loopBound = FLOAT_SPECIES.loopBound(vector.length);
        if (vector.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector vecMeanVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector m2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector norm2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector minVec = FloatVector.broadcast(FLOAT_SPECIES, Float.MAX_VALUE);
            FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, -Float.MAX_VALUE);
            FloatVector centroidDotVec = FloatVector.zero(FLOAT_SPECIES);
            int count = 0;
            for (; i < loopBound; i += FLOAT_SPECIES.length()) {
                ++count;
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
                FloatVector c = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                centroidDotVec = fma(v, c, centroidDotVec);
                FloatVector centeredVec = v.sub(c);
                FloatVector deltaVec = centeredVec.sub(vecMeanVec);
                norm2Vec = fma(centeredVec, centeredVec, norm2Vec);
                vecMeanVec = vecMeanVec.add(deltaVec.mul(1f / count));
                FloatVector delta2Vec = centeredVec.sub(vecMeanVec);
                m2Vec = fma(deltaVec, delta2Vec, m2Vec);
                minVec = minVec.min(centeredVec);
                maxVec = maxVec.max(centeredVec);
                centeredVec.intoArray(centered, i);
            }
            min = minVec.reduceLanes(MIN);
            max = maxVec.reduceLanes(MAX);
            norm2 = norm2Vec.reduceLanes(ADD);
            centroidDot = centroidDotVec.reduceLanes(ADD);
            vecMean = vecMeanVec.reduceLanes(ADD) / FLOAT_SPECIES.length();
            FloatVector d2Mean = vecMeanVec.sub(vecMean);
            m2Vec = fma(d2Mean, d2Mean, m2Vec);
            vectCount = count * FLOAT_SPECIES.length();
            vecVar = m2Vec.reduceLanes(ADD);
        }

        float tailMean = 0;
        float tailM2 = 0;
        int tailCount = 0;
        // handle the tail
        for (; i < vector.length; i++) {
            centroidDot = fma(vector[i], centroid[i], centroidDot);
            centered[i] = vector[i] - centroid[i];
            float delta = centered[i] - tailMean;
            ++tailCount;
            tailMean += delta / tailCount;
            float delta2 = centered[i] - tailMean;
            tailM2 = fma(delta, delta2, tailM2);
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
        }
        if (vectCount == 0) {
            vecMean = tailMean;
            vecVar = tailM2;
        } else if (tailCount > 0) {
            int totalCount = tailCount + vectCount;
            assert totalCount == vector.length;
            float alpha = (float) vectCount / totalCount;
            float beta = 1f - alpha;
            float completeMean = alpha * vecMean + beta * tailMean;
            float dMean2Lhs = (vecMean - completeMean) * (vecMean - completeMean);
            float dMean2Rhs = (tailMean - completeMean) * (tailMean - completeMean);
            vecVar = (vecVar + dMean2Lhs) + beta * (tailM2 + dMean2Rhs);
            vecMean = completeMean;
        }
        stats[0] = vecMean;
        stats[1] = vecVar / vector.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
        stats[5] = centroidDot;
    }

    @Override
    public void calculateOSQGridPoints(float[] target, int[] quantize, int points, float[] pts) {
        int i = 0;
        float daa = 0;
        float dab = 0;
        float dbb = 0;
        float dax = 0;
        float dbx = 0;
        float invPmOnes = 1f / (points - 1f);
        // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
        if (target.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector daaVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector dabVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector dbbVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector daxVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector dbxVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector ones = FloatVector.broadcast(FLOAT_SPECIES, 1f);
            FloatVector invPmOnesVec = FloatVector.broadcast(FLOAT_SPECIES, invPmOnes);
            for (; i < FLOAT_SPECIES.loopBound(target.length); i += FLOAT_SPECIES.length()) {
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, target, i);
                FloatVector oVec = IntVector.fromArray(INTEGER_SPECIES, quantize, i).convert(VectorOperators.I2F, 0).reinterpretAsFloats();
                FloatVector sVec = oVec.mul(invPmOnesVec);
                FloatVector smVec = ones.sub(sVec);
                daaVec = fma(smVec, smVec, daaVec);
                dabVec = fma(smVec, sVec, dabVec);
                dbbVec = fma(sVec, sVec, dbbVec);
                daxVec = fma(v, smVec, daxVec);
                dbxVec = fma(v, sVec, dbxVec);
            }
            daa = daaVec.reduceLanes(ADD);
            dab = dabVec.reduceLanes(ADD);
            dbb = dbbVec.reduceLanes(ADD);
            dax = daxVec.reduceLanes(ADD);
            dbx = dbxVec.reduceLanes(ADD);
        }

        for (; i < target.length; i++) {
            float k = quantize[i];
            float s = k * invPmOnes;
            float ms = 1f - s;
            daa = fma(ms, ms, daa);
            dab = fma(ms, s, dab);
            dbb = fma(s, s, dbb);
            dax = fma(ms, target[i], dax);
            dbx = fma(s, target[i], dbx);
        }

        pts[0] = daa;
        pts[1] = dab;
        pts[2] = dbb;
        pts[3] = dax;
        pts[4] = dbx;
    }

    @Override
    public float calculateOSQLoss(
        float[] target,
        float lowerInterval,
        float upperInterval,
        float step,
        float invStep,
        float norm2,
        float lambda,
        int[] quantize
    ) {
        float a = lowerInterval;
        float b = upperInterval;
        float xe = 0f;
        float e = 0f;
        FloatVector xeVec = FloatVector.zero(FLOAT_SPECIES);
        FloatVector eVec = FloatVector.zero(FLOAT_SPECIES);
        int i = 0;
        // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
        if (target.length > 2 * FLOAT_SPECIES.length()) {
            for (; i < FLOAT_SPECIES.loopBound(target.length); i += FLOAT_SPECIES.length()) {
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, target, i);
                FloatVector vClamped = v.max(a).min(b);
                IntVector xiqint = vClamped.sub(a).mul(invStep).add(0.5f).convert(VectorOperators.F2I, 0).reinterpretAsInts();
                xiqint.intoArray(quantize, i);
                FloatVector quantizeVec = xiqint.convert(VectorOperators.I2F, 0).reinterpretAsFloats();
                FloatVector xiq = quantizeVec.mul(step).add(a);
                FloatVector xiiq = v.sub(xiq);
                xeVec = fma(v, xiiq, xeVec);
                eVec = fma(xiiq, xiiq, eVec);
            }
            e = eVec.reduceLanes(ADD);
            xe = xeVec.reduceLanes(ADD);
        }

        for (; i < target.length; i++) {
            quantize[i] = Math.round((Math.min(Math.max(target[i], a), b) - a) * invStep);
            // this is quantizing and then dequantizing the vector
            float xiq = fma(step, quantize[i], a);
            // how much does the de-quantized value differ from the original value
            float xiiq = target[i] - xiq;
            e = fma(xiiq, xiiq, e);
            xe = fma(target[i], xiiq, xe);
        }
        return (1f - lambda) * xe * xe / norm2 + lambda * e;
    }

    @Override
    public float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm) {
        assert v1.length == centroid.length;
        assert v1.length == originalResidual.length;
        float proj = 0;
        float dsq = 0;
        int i = 0;
        if (v1.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector projVec1 = FloatVector.zero(FLOAT_SPECIES);
            FloatVector projVec2 = FloatVector.zero(FLOAT_SPECIES);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
            FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
            int unrolledLimit = FLOAT_SPECIES.loopBound(v1.length) - FLOAT_SPECIES.length();
            for (; i < unrolledLimit; i += 2 * FLOAT_SPECIES.length()) {
                // one
                FloatVector v1Vec0 = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
                FloatVector centroidVec0 = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                FloatVector originalResidualVec0 = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i);
                FloatVector djkVec0 = v1Vec0.sub(centroidVec0);
                projVec1 = fma(djkVec0, originalResidualVec0, projVec1);
                acc1 = fma(djkVec0, djkVec0, acc1);

                // two
                FloatVector v1Vec1 = FloatVector.fromArray(FLOAT_SPECIES, v1, i + FLOAT_SPECIES.length());
                FloatVector centroidVec1 = FloatVector.fromArray(FLOAT_SPECIES, centroid, i + FLOAT_SPECIES.length());
                FloatVector originalResidualVec1 = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i + FLOAT_SPECIES.length());
                FloatVector djkVec1 = v1Vec1.sub(centroidVec1);
                projVec2 = fma(djkVec1, originalResidualVec1, projVec2);
                acc2 = fma(djkVec1, djkVec1, acc2);
            }
            // vector tail
            for (; i < FLOAT_SPECIES.loopBound(v1.length); i += FLOAT_SPECIES.length()) {
                FloatVector v1Vec = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
                FloatVector centroidVec = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                FloatVector originalResidualVec = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i);
                FloatVector djkVec = v1Vec.sub(centroidVec);
                projVec1 = fma(djkVec, originalResidualVec, projVec1);
                acc1 = fma(djkVec, djkVec, acc1);
            }
            proj += projVec1.add(projVec2).reduceLanes(ADD);
            dsq += acc1.add(acc2).reduceLanes(ADD);
        }
        // tail
        for (; i < v1.length; i++) {
            float djk = v1[i] - centroid[i];
            proj = fma(djk, originalResidual[i], proj);
            dsq = fma(djk, djk, dsq);
        }
        return dsq + soarLambda * proj * proj / rnorm;
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

    private static int limit(int length, int sectionSize) {
        return length - (length % sectionSize);
    }

    static int ipByteBit512(byte[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        int i = 0;
        int sum = 0;

        int sectionLength = INT_SPECIES_512.length() * 4;
        if (q.length >= sectionLength) {
            IntVector acc0 = IntVector.zero(INT_SPECIES_512);
            IntVector acc1 = IntVector.zero(INT_SPECIES_512);
            IntVector acc2 = IntVector.zero(INT_SPECIES_512);
            IntVector acc3 = IntVector.zero(INT_SPECIES_512);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var vals0 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_512, q, i).castShape(INT_SPECIES_512, 0);
                var vals1 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_512, q, i + INT_SPECIES_512.length()).castShape(INT_SPECIES_512, 0);
                var vals2 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_512, q, i + INT_SPECIES_512.length() * 2)
                    .castShape(INT_SPECIES_512, 0);
                var vals3 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_512, q, i + INT_SPECIES_512.length() * 3)
                    .castShape(INT_SPECIES_512, 0);

                long maskBits = Long.reverse((long) BitUtil.VH_BE_LONG.get(d, i / 8));
                var mask0 = VectorMask.fromLong(INT_SPECIES_512, maskBits);
                var mask1 = VectorMask.fromLong(INT_SPECIES_512, maskBits >> 16);
                var mask2 = VectorMask.fromLong(INT_SPECIES_512, maskBits >> 32);
                var mask3 = VectorMask.fromLong(INT_SPECIES_512, maskBits >> 48);

                acc0 = acc0.add(vals0, mask0);
                acc1 = acc1.add(vals1, mask1);
                acc2 = acc2.add(vals2, mask2);
                acc3 = acc3.add(vals3, mask3);
            }
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        sectionLength = INT_SPECIES_256.length();
        if (q.length - i >= sectionLength) {
            IntVector acc = IntVector.zero(INT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var vals = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i).castShape(INT_SPECIES_256, 0);

                long maskBits = Integer.reverse(d[i / 8]) >> 24;
                var mask = VectorMask.fromLong(INT_SPECIES_256, maskBits);

                acc = acc.add(vals, mask);
            }
            sum += acc.reduceLanes(VectorOperators.ADD);
        }

        // that should have got them all (q.length is a multiple of 8, which fits in a 256-bit vector)
        assert i == q.length;
        return sum;
    }

    static int ipByteBit256(byte[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        int i = 0;
        int sum = 0;

        int sectionLength = INT_SPECIES_256.length() * 4;
        if (q.length >= sectionLength) {
            IntVector acc0 = IntVector.zero(INT_SPECIES_256);
            IntVector acc1 = IntVector.zero(INT_SPECIES_256);
            IntVector acc2 = IntVector.zero(INT_SPECIES_256);
            IntVector acc3 = IntVector.zero(INT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var vals0 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i).castShape(INT_SPECIES_256, 0);
                var vals1 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i + INT_SPECIES_256.length()).castShape(INT_SPECIES_256, 0);
                var vals2 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i + INT_SPECIES_256.length() * 2)
                    .castShape(INT_SPECIES_256, 0);
                var vals3 = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i + INT_SPECIES_256.length() * 3)
                    .castShape(INT_SPECIES_256, 0);

                long maskBits = Integer.reverse((int) BitUtil.VH_BE_INT.get(d, i / 8));
                var mask0 = VectorMask.fromLong(INT_SPECIES_256, maskBits);
                var mask1 = VectorMask.fromLong(INT_SPECIES_256, maskBits >> 8);
                var mask2 = VectorMask.fromLong(INT_SPECIES_256, maskBits >> 16);
                var mask3 = VectorMask.fromLong(INT_SPECIES_256, maskBits >> 24);

                acc0 = acc0.add(vals0, mask0);
                acc1 = acc1.add(vals1, mask1);
                acc2 = acc2.add(vals2, mask2);
                acc3 = acc3.add(vals3, mask3);
            }
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        sectionLength = INT_SPECIES_256.length();
        if (q.length - i >= sectionLength) {
            IntVector acc = IntVector.zero(INT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var vals = ByteVector.fromArray(BYTE_SPECIES_FOR_INT_256, q, i).castShape(INT_SPECIES_256, 0);

                long maskBits = Integer.reverse(d[i / 8]) >> 24;
                var mask = VectorMask.fromLong(INT_SPECIES_256, maskBits);

                acc = acc.add(vals, mask);
            }
            sum += acc.reduceLanes(VectorOperators.ADD);
        }

        // that should have got them all (q.length is a multiple of 8, which fits in a 256-bit vector)
        assert i == q.length;
        return sum;
    }

    private static final VectorSpecies<Float> FLOAT_SPECIES_512 = FloatVector.SPECIES_512;
    private static final VectorSpecies<Float> FLOAT_SPECIES_256 = FloatVector.SPECIES_256;

    static float ipFloatBit512(float[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        int i = 0;
        float sum = 0;

        int sectionLength = FLOAT_SPECIES_512.length() * 4;
        if (q.length >= sectionLength) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_512);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_512);
            FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES_512);
            FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES_512);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_512, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_512, q, i + FLOAT_SPECIES_512.length());
                var floats2 = FloatVector.fromArray(FLOAT_SPECIES_512, q, i + FLOAT_SPECIES_512.length() * 2);
                var floats3 = FloatVector.fromArray(FLOAT_SPECIES_512, q, i + FLOAT_SPECIES_512.length() * 3);

                long maskBits = Long.reverse((long) BitUtil.VH_BE_LONG.get(d, i / 8));
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_512, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_512, maskBits >> 16);
                var mask2 = VectorMask.fromLong(FLOAT_SPECIES_512, maskBits >> 32);
                var mask3 = VectorMask.fromLong(FLOAT_SPECIES_512, maskBits >> 48);

                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
                acc2 = acc2.add(floats2, mask2);
                acc3 = acc3.add(floats3, mask3);
            }
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        sectionLength = FLOAT_SPECIES_256.length();
        if (q.length - i >= sectionLength) {
            FloatVector acc = FloatVector.zero(FLOAT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);

                long maskBits = Integer.reverse(d[i / 8]) >> 24;
                var mask = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);

                acc = acc.add(floats, mask);
            }
            sum += acc.reduceLanes(VectorOperators.ADD);
        }

        // that should have got them all (q.length is a multiple of 8, which fits in a 256-bit vector)
        assert i == q.length;
        return sum;
    }

    static float ipFloatBit256(float[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        int i = 0;
        float sum = 0;

        int sectionLength = FLOAT_SPECIES_256.length() * 4;
        if (q.length >= sectionLength) {
            FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES_256);
            FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var floats0 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);
                var floats1 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length());
                var floats2 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length() * 2);
                var floats3 = FloatVector.fromArray(FLOAT_SPECIES_256, q, i + FLOAT_SPECIES_256.length() * 3);

                long maskBits = Integer.reverse((int) BitUtil.VH_BE_INT.get(d, i / 8));
                var mask0 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);
                var mask1 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 8);
                var mask2 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 16);
                var mask3 = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits >> 24);

                acc0 = acc0.add(floats0, mask0);
                acc1 = acc1.add(floats1, mask1);
                acc2 = acc2.add(floats2, mask2);
                acc3 = acc3.add(floats3, mask3);
            }
            sum += acc0.reduceLanes(VectorOperators.ADD) + acc1.reduceLanes(VectorOperators.ADD) + acc2.reduceLanes(VectorOperators.ADD)
                + acc3.reduceLanes(VectorOperators.ADD);
        }

        sectionLength = FLOAT_SPECIES_256.length();
        if (q.length - i >= sectionLength) {
            FloatVector acc = FloatVector.zero(FLOAT_SPECIES_256);
            int limit = limit(q.length, sectionLength);
            for (; i < limit; i += sectionLength) {
                var floats = FloatVector.fromArray(FLOAT_SPECIES_256, q, i);

                long maskBits = Integer.reverse(d[i / 8]) >> 24;
                var mask = VectorMask.fromLong(FLOAT_SPECIES_256, maskBits);

                acc = acc.add(floats, mask);
            }
            sum += acc.reduceLanes(VectorOperators.ADD);
        }

        // that should have got them all (q.length is a multiple of 8, which fits in a 256-bit vector)
        assert i == q.length;
        return sum;
    }

    private static final VectorSpecies<Float> PREFERRED_FLOAT_SPECIES = PanamaVectorConstants.PREFERRED_FLOAT_SPECIES;
    private static final VectorSpecies<Byte> BYTE_SPECIES_FOR_PREFFERED_FLOATS;

    static {
        VectorSpecies<Byte> byteForFloat;
        try {
            // calculate vector size to convert from single bytes to 4-byte floats
            byteForFloat = VectorSpecies.of(byte.class, VectorShape.forBitSize(PREFERRED_FLOAT_SPECIES.vectorBitSize() / Float.BYTES));
        } catch (IllegalArgumentException e) {
            // can't get a byte vector size small enough, just use default impl
            byteForFloat = null;
        }
        BYTE_SPECIES_FOR_PREFFERED_FLOATS = byteForFloat;
    }

    public static float ipFloatByteImpl(float[] q, byte[] d) {
        assert BYTE_SPECIES_FOR_PREFFERED_FLOATS != null;
        FloatVector acc = FloatVector.zero(PREFERRED_FLOAT_SPECIES);
        int i = 0;

        int limit = PREFERRED_FLOAT_SPECIES.loopBound(q.length);
        for (; i < limit; i += PREFERRED_FLOAT_SPECIES.length()) {
            FloatVector qv = FloatVector.fromArray(PREFERRED_FLOAT_SPECIES, q, i);
            ByteVector bv = ByteVector.fromArray(BYTE_SPECIES_FOR_PREFFERED_FLOATS, d, i);
            acc = qv.fma(bv.castShape(PREFERRED_FLOAT_SPECIES, 0), acc);
        }

        float sum = acc.reduceLanes(VectorOperators.ADD);

        // handle the tail
        for (; i < q.length; i++) {
            sum += q[i] * d[i];
        }

        return sum;
    }

    @Override
    public int quantizeVectorWithIntervals(float[] vector, int[] destination, float lowInterval, float upperInterval, byte bits) {
        float nSteps = ((1 << bits) - 1);
        float invStep = nSteps / (upperInterval - lowInterval);
        int sumQuery = 0;
        int i = 0;
        if (vector.length > 2 * FLOAT_SPECIES.length()) {
            int limit = FLOAT_SPECIES.loopBound(vector.length);
            FloatVector lowVec = FloatVector.broadcast(FLOAT_SPECIES, lowInterval);
            FloatVector upperVec = FloatVector.broadcast(FLOAT_SPECIES, upperInterval);
            FloatVector invStepVec = FloatVector.broadcast(FLOAT_SPECIES, invStep);
            for (; i < limit; i += FLOAT_SPECIES.length()) {
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
                FloatVector xi = v.max(lowVec).min(upperVec); // clamp
                // round
                IntVector assignment = xi.sub(lowVec).mul(invStepVec).add(0.5f).convert(VectorOperators.F2I, 0).reinterpretAsInts();
                sumQuery += assignment.reduceLanes(ADD);
                assignment.intoArray(destination, i);
            }
        }
        for (; i < vector.length; i++) {
            float xi = Math.min(Math.max(vector[i], lowInterval), upperInterval);
            int assignment = Math.round((xi - lowInterval) * invStep);
            sumQuery += assignment;
            destination[i] = assignment;
        }
        return sumQuery;
    }

    @Override
    public void squareDistanceBulk(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int distancesOffset, float[] distances) {
        squareDistanceBulk(query, 0, query.length, v0, v1, v2, v3, distancesOffset, distances);
    }

    @Override
    public void squareDistanceBulk(
        float[] query,
        int queryOffset,
        int length,
        float[] v0,
        float[] v1,
        float[] v2,
        float[] v3,
        int distancesOffset,
        float[] distances
    ) {
        FloatVector sv0 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector sv1 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector sv2 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector sv3 = FloatVector.zero(FLOAT_SPECIES);
        final int end = queryOffset + length;
        final int vectorEnd = queryOffset + FLOAT_SPECIES.loopBound(length);
        int i = queryOffset;
        for (; i < vectorEnd; i += FLOAT_SPECIES.length()) {
            FloatVector qv = FloatVector.fromArray(FLOAT_SPECIES, query, i);
            FloatVector dv0 = FloatVector.fromArray(FLOAT_SPECIES, v0, i);
            FloatVector dv1 = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
            FloatVector dv2 = FloatVector.fromArray(FLOAT_SPECIES, v2, i);
            FloatVector dv3 = FloatVector.fromArray(FLOAT_SPECIES, v3, i);
            FloatVector diff0 = qv.sub(dv0);
            sv0 = fma(diff0, diff0, sv0);
            FloatVector diff1 = qv.sub(dv1);
            sv1 = fma(diff1, diff1, sv1);
            FloatVector diff2 = qv.sub(dv2);
            sv2 = fma(diff2, diff2, sv2);
            FloatVector diff3 = qv.sub(dv3);
            sv3 = fma(diff3, diff3, sv3);
        }
        float distance0 = sv0.reduceLanes(VectorOperators.ADD);
        float distance1 = sv1.reduceLanes(VectorOperators.ADD);
        float distance2 = sv2.reduceLanes(VectorOperators.ADD);
        float distance3 = sv3.reduceLanes(VectorOperators.ADD);

        for (; i < end; i++) {
            final float qValue = query[i];
            final float diff0 = qValue - v0[i];
            final float diff1 = qValue - v1[i];
            final float diff2 = qValue - v2[i];
            final float diff3 = qValue - v3[i];
            distance0 = fma(diff0, diff0, distance0);
            distance1 = fma(diff1, diff1, distance1);
            distance2 = fma(diff2, diff2, distance2);
            distance3 = fma(diff3, diff3, distance3);
        }
        distances[distancesOffset] = distance0;
        distances[distancesOffset + 1] = distance1;
        distances[distancesOffset + 2] = distance2;
        distances[distancesOffset + 3] = distance3;
    }

    @Override
    public void soarDistanceBulk(
        float[] v1,
        float[] c0,
        float[] c1,
        float[] c2,
        float[] c3,
        float[] originalResidual,
        float soarLambda,
        float rnorm,
        float[] distances
    ) {

        FloatVector projVec0 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector projVec1 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector projVec2 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector projVec3 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector acc0 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector acc1 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector acc2 = FloatVector.zero(FLOAT_SPECIES);
        FloatVector acc3 = FloatVector.zero(FLOAT_SPECIES);
        final int limit = FLOAT_SPECIES.loopBound(v1.length);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector v1Vec = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
            FloatVector c0Vec = FloatVector.fromArray(FLOAT_SPECIES, c0, i);
            FloatVector c1Vec = FloatVector.fromArray(FLOAT_SPECIES, c1, i);
            FloatVector c2Vec = FloatVector.fromArray(FLOAT_SPECIES, c2, i);
            FloatVector c3Vec = FloatVector.fromArray(FLOAT_SPECIES, c3, i);
            FloatVector originalResidualVec = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i);
            FloatVector djkVec0 = v1Vec.sub(c0Vec);
            FloatVector djkVec1 = v1Vec.sub(c1Vec);
            FloatVector djkVec2 = v1Vec.sub(c2Vec);
            FloatVector djkVec3 = v1Vec.sub(c3Vec);
            projVec0 = fma(djkVec0, originalResidualVec, projVec0);
            projVec1 = fma(djkVec1, originalResidualVec, projVec1);
            projVec2 = fma(djkVec2, originalResidualVec, projVec2);
            projVec3 = fma(djkVec3, originalResidualVec, projVec3);
            acc0 = fma(djkVec0, djkVec0, acc0);
            acc1 = fma(djkVec1, djkVec1, acc1);
            acc2 = fma(djkVec2, djkVec2, acc2);
            acc3 = fma(djkVec3, djkVec3, acc3);
        }
        float proj0 = projVec0.reduceLanes(ADD);
        float dsq0 = acc0.reduceLanes(ADD);
        float proj1 = projVec1.reduceLanes(ADD);
        float dsq1 = acc1.reduceLanes(ADD);
        float proj2 = projVec2.reduceLanes(ADD);
        float dsq2 = acc2.reduceLanes(ADD);
        float proj3 = projVec3.reduceLanes(ADD);
        float dsq3 = acc3.reduceLanes(ADD);
        // tail
        for (; i < v1.length; i++) {
            float v = v1[i];
            float djk0 = v - c0[i];
            float djk1 = v - c1[i];
            float djk2 = v - c2[i];
            float djk3 = v - c3[i];
            proj0 = fma(djk0, originalResidual[i], proj0);
            proj1 = fma(djk1, originalResidual[i], proj1);
            proj2 = fma(djk2, originalResidual[i], proj2);
            proj3 = fma(djk3, originalResidual[i], proj3);
            dsq0 = fma(djk0, djk0, dsq0);
            dsq1 = fma(djk1, djk1, dsq1);
            dsq2 = fma(djk2, djk2, dsq2);
            dsq3 = fma(djk3, djk3, dsq3);
        }
        distances[0] = dsq0 + soarLambda * proj0 * proj0 / rnorm;
        distances[1] = dsq1 + soarLambda * proj1 * proj1 / rnorm;
        distances[2] = dsq2 + soarLambda * proj2 * proj2 / rnorm;
        distances[3] = dsq3 + soarLambda * proj3 * proj3 / rnorm;
    }

    private static final VectorSpecies<Integer> INT_SPECIES_128 = IntVector.SPECIES_128;
    private static final IntVector SHIFTS_256;
    private static final IntVector HIGH_SHIFTS_128;
    private static final IntVector LOW_SHIFTS_128;
    static {
        final int[] shifts = new int[] { 7, 6, 5, 4, 3, 2, 1, 0 };
        if (VECTOR_BITSIZE == 128) {
            HIGH_SHIFTS_128 = IntVector.fromArray(INT_SPECIES_128, shifts, 0);
            LOW_SHIFTS_128 = IntVector.fromArray(INT_SPECIES_128, shifts, INT_SPECIES_128.length());
            SHIFTS_256 = null;
        } else {
            SHIFTS_256 = IntVector.fromArray(INT_SPECIES_256, shifts, 0);
            HIGH_SHIFTS_128 = null;
            LOW_SHIFTS_128 = null;
        }
    }
    private static final int[] SHIFTS = new int[] { 7, 6, 5, 4, 3, 2, 1, 0 };

    @Override
    public void packAsBinary(int[] vector, byte[] packed) {
        // 128 / 32 == 4
        if (vector.length >= 8 && HAS_FAST_INTEGER_VECTORS) {
            // TODO: can we optimize for >= 512?
            if (VECTOR_BITSIZE >= 256) {
                packAsBinary256(vector, packed);
                return;
            } else if (VECTOR_BITSIZE == 128) {
                packAsBinary128(vector, packed);
                return;
            }
        }
        DefaultESVectorUtilSupport.packAsBinaryImpl(vector, packed);
    }

    private void packAsBinary256(int[] vector, byte[] packed) {
        final int limit = INT_SPECIES_256.loopBound(vector.length);
        int i = 0;
        int index = 0;
        for (; i < limit; i += INT_SPECIES_256.length(), index++) {
            IntVector v = IntVector.fromArray(INT_SPECIES_256, vector, i);
            int result = v.lanewise(LSHL, SHIFTS_256).reduceLanes(OR);
            packed[index] = (byte) result;
        }
        if (i == vector.length) {
            return; // all done
        }
        byte result = 0;
        for (int j = 7; j >= 0 && i < vector.length; i++, j--) {
            assert vector[i] == 0 || vector[i] == 1;
            result |= (byte) ((vector[i] & 1) << j);
        }
        packed[index] = result;
    }

    private void packAsBinary128(int[] vector, byte[] packed) {
        final int limit = INT_SPECIES_128.loopBound(vector.length) - INT_SPECIES_128.length();
        int i = 0;
        int index = 0;
        for (; i < limit; i += 2 * INT_SPECIES_128.length(), index++) {
            IntVector v = IntVector.fromArray(INT_SPECIES_128, vector, i);
            var v1 = v.lanewise(LSHL, HIGH_SHIFTS_128);
            v = IntVector.fromArray(INT_SPECIES_128, vector, i + INT_SPECIES_128.length());
            var v2 = v.lanewise(LSHL, LOW_SHIFTS_128);
            int result = v1.lanewise(OR, v2).reduceLanes(OR);
            packed[index] = (byte) result;
        }
        if (i == vector.length) {
            return; // all done
        }
        byte result = 0;
        for (int j = 7; j >= 0 && i < vector.length; i++, j--) {
            assert vector[i] == 0 || vector[i] == 1;
            result |= (byte) ((vector[i] & 1) << j);
        }
        packed[index] = result;
    }

    @Override
    public void packDibit(int[] vector, byte[] packed) {
        // TODO
        DefaultESVectorUtilSupport.packDibitImpl(vector, packed);
    }

    @Override
    public void transposeHalfByte(int[] q, byte[] quantQueryByte) {
        // 128 / 32 == 4
        if (q.length >= 8 && HAS_FAST_INTEGER_VECTORS) {
            if (VECTOR_BITSIZE >= 256) {
                transposeHalfByte256(q, quantQueryByte);
                return;
            } else if (VECTOR_BITSIZE == 128) {
                transposeHalfByte128(q, quantQueryByte);
                return;
            }
        }
        DefaultESVectorUtilSupport.transposeHalfByteImpl(q, quantQueryByte);
    }

    private void transposeHalfByte256(int[] q, byte[] quantQueryByte) {
        final int limit = INT_SPECIES_256.loopBound(q.length);
        int i = 0;
        int index = 0;
        for (; i < limit; i += INT_SPECIES_256.length(), index++) {
            IntVector v = IntVector.fromArray(INT_SPECIES_256, q, i);

            int lowerByte = v.and(1).lanewise(LSHL, SHIFTS_256).reduceLanes(VectorOperators.OR);
            int lowerMiddleByte = v.lanewise(ASHR, 1).and(1).lanewise(LSHL, SHIFTS_256).reduceLanes(VectorOperators.OR);
            int upperMiddleByte = v.lanewise(ASHR, 2).and(1).lanewise(LSHL, SHIFTS_256).reduceLanes(VectorOperators.OR);
            int upperByte = v.lanewise(ASHR, 3).and(1).lanewise(LSHL, SHIFTS_256).reduceLanes(VectorOperators.OR);

            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;

        }
        if (i == q.length) {
            return; // all done
        }
        int lowerByte = 0;
        int lowerMiddleByte = 0;
        int upperMiddleByte = 0;
        int upperByte = 0;
        for (int j = 7; i < q.length; j--, i++) {
            lowerByte |= (q[i] & 1) << j;
            lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
            upperMiddleByte |= ((q[i] >> 2) & 1) << j;
            upperByte |= ((q[i] >> 3) & 1) << j;
        }
        quantQueryByte[index] = (byte) lowerByte;
        quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
        quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
        quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
    }

    private void transposeHalfByte128(int[] q, byte[] quantQueryByte) {
        final int limit = INT_SPECIES_128.loopBound(q.length) - INT_SPECIES_128.length();
        int i = 0;
        int index = 0;
        for (; i < limit; i += 2 * INT_SPECIES_128.length(), index++) {
            IntVector v = IntVector.fromArray(INT_SPECIES_128, q, i);

            var lowerByteHigh = v.and(1).lanewise(LSHL, HIGH_SHIFTS_128);
            var lowerMiddleByteHigh = v.lanewise(ASHR, 1).and(1).lanewise(LSHL, HIGH_SHIFTS_128);
            var upperMiddleByteHigh = v.lanewise(ASHR, 2).and(1).lanewise(LSHL, HIGH_SHIFTS_128);
            var upperByteHigh = v.lanewise(ASHR, 3).and(1).lanewise(LSHL, HIGH_SHIFTS_128);

            v = IntVector.fromArray(INT_SPECIES_128, q, i + INT_SPECIES_128.length());
            var lowerByteLow = v.and(1).lanewise(LSHL, LOW_SHIFTS_128);
            var lowerMiddleByteLow = v.lanewise(ASHR, 1).and(1).lanewise(LSHL, LOW_SHIFTS_128);
            var upperMiddleByteLow = v.lanewise(ASHR, 2).and(1).lanewise(LSHL, LOW_SHIFTS_128);
            var upperByteLow = v.lanewise(ASHR, 3).and(1).lanewise(LSHL, LOW_SHIFTS_128);

            int lowerByte = lowerByteHigh.lanewise(OR, lowerByteLow).reduceLanes(OR);
            int lowerMiddleByte = lowerMiddleByteHigh.lanewise(OR, lowerMiddleByteLow).reduceLanes(OR);
            int upperMiddleByte = upperMiddleByteHigh.lanewise(OR, upperMiddleByteLow).reduceLanes(OR);
            int upperByte = upperByteHigh.lanewise(OR, upperByteLow).reduceLanes(OR);

            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;

        }
        if (i == q.length) {
            return; // all done
        }
        int lowerByte = 0;
        int lowerMiddleByte = 0;
        int upperMiddleByte = 0;
        int upperByte = 0;
        for (int j = 7; i < q.length; j--, i++) {
            lowerByte |= (q[i] & 1) << j;
            lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
            upperMiddleByte |= ((q[i] >> 2) & 1) << j;
            upperByte |= ((q[i] >> 3) & 1) << j;
        }
        quantQueryByte[index] = (byte) lowerByte;
        quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
        quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
        quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
    }

    private static final VectorSpecies<Byte> PREFERRED_BYTE_SPECIES = PanamaVectorConstants.PREFERRED_BYTE_SPECIES;

    @Override
    public int indexOf(final byte[] bytes, final int offset, final int length, final byte marker) {
        final ByteVector markerVector = ByteVector.broadcast(PREFERRED_BYTE_SPECIES, marker);
        final int loopBound = PREFERRED_BYTE_SPECIES.loopBound(length);
        for (int i = 0; i < loopBound; i += PREFERRED_BYTE_SPECIES.length()) {
            ByteVector chunk = ByteVector.fromArray(PREFERRED_BYTE_SPECIES, bytes, offset + i);
            VectorMask<Byte> mask = chunk.eq(markerVector);
            if (mask.anyTrue()) {
                return i + mask.firstTrue();
            }
        }
        // tail
        if (loopBound < length) {
            int remaining = length - loopBound;
            int tail = ByteArrayUtils.indexOf(bytes, offset + loopBound, remaining, marker);
            if (tail >= 0) {
                return loopBound + tail;
            }
        }
        return -1;
    }

    @Override
    public boolean contains(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength) {
        // Scalar logic is faster for short values (below approximately 24 bytes)
        if (valueLength < 24) {
            return ByteArrayUtils.contains(value, valueOffset, valueLength, term, termOffset, termLength);
        }

        byte first = term[termOffset];
        byte last = term[termOffset + termLength - 1];
        int maxPos = valueOffset + valueLength - termLength;

        ByteVector firstVec = ByteVector.broadcast(PREFERRED_BYTE_SPECIES, first);
        ByteVector lastVec = ByteVector.broadcast(PREFERRED_BYTE_SPECIES, last);
        int vectorSize = PREFERRED_BYTE_SPECIES.length();
        int i = valueOffset;
        int loopBound = maxPos - vectorSize + 1;
        for (; i <= loopBound; i += vectorSize) {
            ByteVector blockFirst = ByteVector.fromArray(PREFERRED_BYTE_SPECIES, value, i);
            ByteVector blockLast = ByteVector.fromArray(PREFERRED_BYTE_SPECIES, value, i + termLength - 1);
            long mask = blockFirst.eq(firstVec).and(blockLast.eq(lastVec)).toLong();
            while (mask != 0) {
                int pos = Long.numberOfTrailingZeros(mask);
                int absPos = i + pos;
                if (absPos > maxPos) {
                    break;
                }
                if (middleBytesMatch(value, absPos, term, termOffset, termLength)) {
                    return true;
                }
                mask &= mask - 1;
            }
        }
        return ByteArrayUtils.contains(value, i, valueOffset + valueLength - i, term, termOffset, termLength);
    }

    /** Checks bytes between first and last (exclusive) since those were already verified by the SIMD masks. */
    private static boolean middleBytesMatch(byte[] value, int valuePos, byte[] term, int termOffset, int termLength) {
        for (int k = 1; k < termLength - 1; k++) {
            if (value[valuePos + k] != term[termOffset + k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int codePointCount(final BytesRef bytesRef) {
        // SWAR logic is faster for lengths below approximately 54
        if (bytesRef.length < 54) {
            return ByteArrayUtils.codePointCount(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        }

        int continuations = 0;
        int highBits = 0xC0;
        int continuationByte = 0x80; // continuation bytes have first bit set and second bit unset
        final ByteVector highBitsVec = ByteVector.broadcast(PREFERRED_BYTE_SPECIES, (byte) highBits);
        final ByteVector continuationVec = ByteVector.broadcast(PREFERRED_BYTE_SPECIES, (byte) continuationByte);
        final int loopBound = PREFERRED_BYTE_SPECIES.loopBound(bytesRef.length);
        int i = 0;
        for (; i < loopBound; i += PREFERRED_BYTE_SPECIES.length()) {
            ByteVector chunk = ByteVector.fromArray(PREFERRED_BYTE_SPECIES, bytesRef.bytes, bytesRef.offset + i);
            VectorMask<Byte> mask = chunk.and(highBitsVec).eq(continuationVec);
            continuations += mask.trueCount();
        }

        // tail
        for (int pos = bytesRef.offset + loopBound; pos < bytesRef.offset + bytesRef.length; pos++) {
            continuations += (bytesRef.bytes[pos] & highBits) == continuationByte ? 1 : 0;
        }

        return bytesRef.length - continuations;
    }

    @Override
    public void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest) {
        assert other.length == dest.length;

        final FloatVector scaleDestVec = FloatVector.broadcast(FLOAT_SPECIES, scaleDest);
        final int limit = FLOAT_SPECIES.loopBound(dest.length);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector destVec = FloatVector.fromArray(FLOAT_SPECIES, dest, i);
            FloatVector otherVec = FloatVector.fromArray(FLOAT_SPECIES, other, i);
            destVec = fma(destVec, scaleDestVec, otherVec.mul(scaleOther));
            destVec.intoArray(dest, i);
        }

        // tail
        for (; i < dest.length; i++) {
            dest[i] = fma(scaleOther, other[i], scaleDest * dest[i]);
        }
    }

    @Override
    public void linearCombination(float scaleOther, float[] other, float[] dest) {
        assert other.length == dest.length;

        final FloatVector scaleOtherVec = FloatVector.broadcast(FLOAT_SPECIES, scaleOther);
        final int limit = FLOAT_SPECIES.loopBound(dest.length);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector destVec = FloatVector.fromArray(FLOAT_SPECIES, dest, i);
            FloatVector otherVec = FloatVector.fromArray(FLOAT_SPECIES, other, i);
            destVec = fma(otherVec, scaleOtherVec, destVec);
            destVec.intoArray(dest, i);
        }

        // tail
        for (; i < dest.length; i++) {
            dest[i] = fma(other[i], scaleOther, dest[i]);
        }
    }

    @Override
    public float logSumExpNQT(float[] vector) {
        assert vector.length > 0;

        // Uses a vectorized implementation of a
        // <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming algorithm</a>.
        FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, Float.NEGATIVE_INFINITY);
        FloatVector sum = FloatVector.broadcast(FLOAT_SPECIES, 0);

        final int limit = FLOAT_SPECIES.loopBound(vector.length);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector vec = FloatVector.fromArray(FLOAT_SPECIES, vector, i);
            FloatVector newMaxVec = maxVec.max(vec);
            sum = sum.mul(pow2NQT(maxVec.sub(newMaxVec))).add(pow2NQT(vec.sub(newMaxVec)));
            maxVec = newMaxVec;
        }

        // In the case vector.length >= FLOAT_SPECIES.length(), each lane has its own max, so we need to use the same one throughout
        float maxVal = maxVec.reduceLanes(MAX);
        float reducedSum = reduceLogSumExpLanes(sum, maxVec, maxVal);

        // tail
        for (; i < vector.length; i++) {
            float v = vector[i];
            float newMaxVal = Math.max(maxVal, v);
            reducedSum *= MathUtils.pow2NQT(maxVal - newMaxVal);
            reducedSum += MathUtils.pow2NQT(v - newMaxVal);
            maxVal = newMaxVal;
        }

        return maxVal + MathUtils.log2NQT(reducedSum);
    }

    @Override
    public float logSumExpNQTDiff(float[] v1, float[] v2, float eps) {
        assert v1.length > 0;
        assert v1.length == v2.length;

        // Uses a vectorized implementation of a
        // <a href="https://www.nowozin.net/sebastian/blog/streaming-log-sum-exp-computation.html">streaming algorithm</a>.
        FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, Float.NEGATIVE_INFINITY);
        FloatVector sum = FloatVector.broadcast(FLOAT_SPECIES, 0);

        final int limit = FLOAT_SPECIES.loopBound(v1.length);
        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector vec1 = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
            FloatVector vec2 = FloatVector.fromArray(FLOAT_SPECIES, v2, i);
            FloatVector vec = vec1.sub(vec2).div(eps);

            FloatVector newMaxVec = maxVec.max(vec);
            sum = sum.mul(pow2NQT(maxVec.sub(newMaxVec))).add(pow2NQT(vec.sub(newMaxVec)));
            maxVec = newMaxVec;
        }

        // In the case vector.length >= FLOAT_SPECIES.length(), each lane has its own max, so we need to use the same one throughout
        float maxVal = maxVec.reduceLanes(MAX);
        float reducedSum = reduceLogSumExpLanes(sum, maxVec, maxVal);

        // tail
        for (; i < v1.length; i++) {
            float v = (v1[i] - v2[i]) / eps;
            float newMaxVal = Math.max(maxVal, v);
            reducedSum *= MathUtils.pow2NQT(maxVal - newMaxVal);
            reducedSum += MathUtils.pow2NQT(v - newMaxVal);
            maxVal = newMaxVal;
        }

        return maxVal + MathUtils.log2NQT(reducedSum);
    }

    private static float reduceLogSumExpLanes(FloatVector sum, FloatVector maxVec, float maxVal) {
        // In the case vector.length >= FLOAT_SPECIES.length(), each lane has its own max, so we need to use the same one throughout
        float reducedSum = 0;
        // If vector.length < FLOAT_SPECIES.length(), maxVal == Float.NEGATIVE_INFINITY
        if (Float.isFinite(maxVal)) {
            sum = sum.mul(pow2NQT(maxVec.sub(maxVal)));
            reducedSum = sum.reduceLanes(ADD);
        }
        return reducedSum;
    }

    @Override
    public void pow2DiffAndScaleNQT(float[] v1, float[] v2, float a, float eps, float[] result) {
        assert v1.length > 0;
        assert v1.length == v2.length;
        assert v1.length == result.length;

        final int limit = FLOAT_SPECIES.loopBound(v1.length);
        FloatVector base = FloatVector.broadcast(FLOAT_SPECIES, (float) 2);

        int i = 0;
        for (; i < limit; i += FLOAT_SPECIES.length()) {
            FloatVector vec1 = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
            FloatVector vec2 = FloatVector.fromArray(FLOAT_SPECIES, v2, i);
            pow2NQT(vec1.sub(vec2).add(a).div(eps)).intoArray(result, i);
        }

        for (; i < v1.length; i++) {
            result[i] = MathUtils.pow2NQT((a + v1[i] - v2[i]) / eps);
        }
    }

    // Computes pow(2, exponent) using the NQT approximation
    static FloatVector pow2NQT(FloatVector exponent) {
        IntVector ones = IntVector.broadcast(INTEGER_SPECIES, 1);
        IntVector negOnes = IntVector.broadcast(INTEGER_SPECIES, -1);
        IntVector signs = ones.blend(negOnes, exponent.compare(VectorOperators.LT, 0.0f).cast(INTEGER_SPECIES));

        // The next line implements the floor(exponent + 1)
        IntVector p = (IntVector) exponent.lanewise(VectorOperators.ABS)
            .add(1, exponent.compare(VectorOperators.GT, 0.0f))
            .convert(VectorOperators.F2I, 0)
            .mul(signs);
        p = p.max(-30).min(30);
        FloatVector pFloat = (FloatVector) p.convert(VectorOperators.I2F, 0);
        // Replace div(2) with mul(0.5f)
        FloatVector m = exponent.sub(pFloat).mul(0.5f).add(1.0f);
        // Build 2^p using direct IEEE-754 bit manipulation
        // Add EXPONENT_BIAS and shift left by MANTISSA_BITS bits to hit the float exponent field
        IntVector pBits = p.add(MathUtils.EXPONENT_BIAS).lanewise(VectorOperators.LSHL, MathUtils.MANTISSA_BITS);
        FloatVector powerOf2 = pBits.reinterpretAsFloats();
        return m.mul(powerOf2).max(0.0f);
    }

    private static boolean canUseBulkPath(MultiVectorsSource<?> source) {
        return SUPPORTS_NATIVE_VECTORS
            && SUPPORTS_HEAP_SEGMENTS
            && source.vectorBytes() != null
            && source.vectorCount() > 0
            && source.vectorBytes().length == source.vectorCount() * source.vectorByteSize();
    }

    private static boolean canUseF32BulkPath(MultiFloatVectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims() * Float.BYTES;
    }

    private static boolean canUseBFloat16Path(MultiBFloat16VectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims() * Short.BYTES;
    }

    private static boolean canUseI8BulkPath(MultiByteVectorsSource source) {
        return canUseBulkPath(source) && source.vectorByteSize() == source.vectorDims();
    }

}
