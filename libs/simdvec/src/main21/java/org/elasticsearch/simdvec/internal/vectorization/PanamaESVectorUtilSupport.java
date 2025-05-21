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

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.MAX;
import static jdk.incubator.vector.VectorOperators.MIN;

public final class PanamaESVectorUtilSupport implements ESVectorUtilSupport {

    static final int VECTOR_BITSIZE;

    private static final VectorSpecies<Float> FLOAT_SPECIES;
    /** Whether integer vectors can be trusted to actually be fast. */
    static final boolean HAS_FAST_INTEGER_VECTORS;

    static {
        // default to platform supported bitsize
        VECTOR_BITSIZE = VectorShape.preferredShape().vectorBitSize();
        FLOAT_SPECIES = VectorSpecies.of(float.class, VectorShape.forBitSize(VECTOR_BITSIZE));

        // hotspot misses some SSE intrinsics, workaround it
        // to be fair, they do document this thing only works well with AVX2/AVX3 and Neon
        boolean isAMD64withoutAVX2 = Constants.OS_ARCH.equals("amd64") && VECTOR_BITSIZE < 256;
        HAS_FAST_INTEGER_VECTORS = isAMD64withoutAVX2 == false;
    }

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
                vecMeanVec = vecMeanVec.add(deltaVec.div(count));
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
                vecMeanVec = vecMeanVec.add(deltaVec.div(count));
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
    public void calculateOSQGridPoints(float[] target, float[] interval, int points, float invStep, float[] pts) {
        float a = interval[0];
        float b = interval[1];
        int i = 0;
        float daa = 0;
        float dab = 0;
        float dbb = 0;
        float dax = 0;
        float dbx = 0;

        FloatVector daaVec = FloatVector.zero(FLOAT_SPECIES);
        FloatVector dabVec = FloatVector.zero(FLOAT_SPECIES);
        FloatVector dbbVec = FloatVector.zero(FLOAT_SPECIES);
        FloatVector daxVec = FloatVector.zero(FLOAT_SPECIES);
        FloatVector dbxVec = FloatVector.zero(FLOAT_SPECIES);

        // if the array size is large (> 2x platform vector size), it's worth the overhead to vectorize
        if (target.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector ones = FloatVector.broadcast(FLOAT_SPECIES, 1f);
            FloatVector pmOnes = FloatVector.broadcast(FLOAT_SPECIES, points - 1f);
            for (; i < FLOAT_SPECIES.loopBound(target.length); i += FLOAT_SPECIES.length()) {
                FloatVector v = FloatVector.fromArray(FLOAT_SPECIES, target, i);
                FloatVector vClamped = v.max(a).min(b);
                Vector<Integer> xiqint = vClamped.sub(a)
                    .mul(invStep)
                    // round
                    .add(0.5f)
                    .convert(VectorOperators.F2I, 0);
                FloatVector kVec = xiqint.convert(VectorOperators.I2F, 0).reinterpretAsFloats();
                FloatVector sVec = kVec.div(pmOnes);
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
            float k = Math.round((Math.min(Math.max(target[i], a), b) - a) * invStep);
            float s = k / (points - 1);
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
    public float calculateOSQLoss(float[] target, float[] interval, float step, float invStep, float norm2, float lambda) {
        float a = interval[0];
        float b = interval[1];
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
                Vector<Integer> xiqint = vClamped.sub(a).mul(invStep).add(0.5f).convert(VectorOperators.F2I, 0);
                FloatVector xiq = xiqint.convert(VectorOperators.I2F, 0).reinterpretAsFloats().mul(step).add(a);
                FloatVector xiiq = v.sub(xiq);
                xeVec = fma(v, xiiq, xeVec);
                eVec = fma(xiiq, xiiq, eVec);
            }
            e = eVec.reduceLanes(ADD);
            xe = xeVec.reduceLanes(ADD);
        }

        for (; i < target.length; i++) {
            // this is quantizing and then dequantizing the vector
            float xiq = fma(step, Math.round((Math.min(Math.max(target[i], a), b) - a) * invStep), a);
            // how much does the de-quantized value differ from the original value
            float xiiq = target[i] - xiq;
            e = fma(xiiq, xiiq, e);
            xe = fma(target[i], xiiq, xe);
        }
        return (1f - lambda) * xe * xe / norm2 + lambda * e;
    }

    @Override
    public float soarResidual(float[] v1, float[] centroid, float[] originalResidual) {
        assert v1.length == centroid.length;
        assert v1.length == originalResidual.length;
        float proj = 0;
        int i = 0;
        if (v1.length > 2 * FLOAT_SPECIES.length()) {
            FloatVector projVec1 = FloatVector.zero(FLOAT_SPECIES);
            FloatVector projVec2 = FloatVector.zero(FLOAT_SPECIES);
            int unrolledLimit = FLOAT_SPECIES.loopBound(v1.length) - FLOAT_SPECIES.length();
            for (; i < unrolledLimit; i += 2 * FLOAT_SPECIES.length()) {
                // one
                FloatVector v1Vec0 = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
                FloatVector centroidVec0 = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                FloatVector originalResidualVec0 = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i);
                FloatVector djkVec0 = v1Vec0.sub(centroidVec0);
                projVec1 = fma(djkVec0, originalResidualVec0, projVec1);

                // two
                FloatVector v1Vec1 = FloatVector.fromArray(FLOAT_SPECIES, v1, i + FLOAT_SPECIES.length());
                FloatVector centroidVec1 = FloatVector.fromArray(FLOAT_SPECIES, centroid, i + FLOAT_SPECIES.length());
                FloatVector originalResidualVec1 = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i + FLOAT_SPECIES.length());
                FloatVector djkVec1 = v1Vec1.sub(centroidVec1);
                projVec2 = fma(djkVec1, originalResidualVec1, projVec2);
            }
            // vector tail
            for (; i < FLOAT_SPECIES.loopBound(v1.length); i += FLOAT_SPECIES.length()) {
                FloatVector v1Vec = FloatVector.fromArray(FLOAT_SPECIES, v1, i);
                FloatVector centroidVec = FloatVector.fromArray(FLOAT_SPECIES, centroid, i);
                FloatVector originalResidualVec = FloatVector.fromArray(FLOAT_SPECIES, originalResidual, i);
                FloatVector djkVec = v1Vec.sub(centroidVec);
                projVec1 = fma(djkVec, originalResidualVec, projVec1);
            }
            proj += projVec1.add(projVec2).reduceLanes(ADD);
        }
        // tail
        for (; i < v1.length; i++) {
            float djk = v1[i] - centroid[i];
            proj = fma(djk, originalResidual[i], proj);
        }
        return proj;
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

    private static final VectorSpecies<Float> PREFERRED_FLOAT_SPECIES = FloatVector.SPECIES_PREFERRED;
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
}
