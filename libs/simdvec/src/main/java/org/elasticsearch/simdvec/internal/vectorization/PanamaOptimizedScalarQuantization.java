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
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import org.elasticsearch.simdvec.OptimizedScalarQuantization;

import static jdk.incubator.vector.VectorOperators.ADD;
import static jdk.incubator.vector.VectorOperators.MAX;
import static jdk.incubator.vector.VectorOperators.MIN;
import static org.elasticsearch.simdvec.internal.vectorization.PanamaESVectorUtilSupport.fma;

/**
 * Panama Vector API implementations of the OSQ methods.
 * <p>
 * See the notes on implementing SIMD with Panama at the top of {@link PanamaESVectorUtilSupport}
 */
public class PanamaOptimizedScalarQuantization extends OptimizedScalarQuantization {

    private static final VectorSpecies<Float> FLOAT_SPECIES = PanamaVectorConstants.PREFERRED_FLOAT_SPECIES;
    private static final VectorSpecies<Byte> BYTE_SPECIES = PanamaVectorConstants.PREFERRED_BYTE_SPECIES;
    private static final VectorSpecies<Integer> INTEGER_SPECIES = PanamaVectorConstants.PREFERRED_INTEGER_SPECIES;
    private static final VectorSpecies<Byte> BYTES_FOR_4BYTE_SPECIES = PanamaVectorConstants.BYTES_FOR_4BYTE_SPECIES;

    @Override
    public void centerAndCalculateStatsEuclidean(float[] vector, float[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length : "vector dimensions differ: " + vector.length + "!=" + centroid.length;
        assert vector.length == centered.length : "vector dimensions differ: " + vector.length + "!=" + centered.length;
        assert stats.length == 5;

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
            m2Vec = m2Vec.add(d2Mean.mul(d2Mean).mul(count));
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
            vecVar = vecVar + tailM2 + vectCount * dMean2Lhs + tailCount * dMean2Rhs;
            vecMean = completeMean;
        }
        stats[0] = vecMean;
        stats[1] = vecVar / vector.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    @Override
    public void centerAndCalculateStatsDp(float[] vector, float[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length : "vector dimensions differ: " + vector.length + "!=" + centroid.length;
        assert vector.length == centered.length : "vector dimensions differ: " + vector.length + "!=" + centered.length;
        assert stats.length == 6;

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
            m2Vec = m2Vec.add(d2Mean.mul(d2Mean).mul(count));
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
            vecVar = vecVar + tailM2 + vectCount * dMean2Lhs + tailCount * dMean2Rhs;
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
    public void centerAndCalculateStatsEuclidean(byte[] vector, byte[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length : "vector dimensions differ: " + vector.length + "!=" + centroid.length;
        assert vector.length == centered.length : "vector dimensions differ: " + vector.length + "!=" + centered.length;
        assert stats.length == 5;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        int i = 0;
        int vectCount = 0;
        if (vector.length >= BYTES_FOR_4BYTE_SPECIES.length()) {
            FloatVector vecMeanVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector m2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector norm2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector minVec = FloatVector.broadcast(FLOAT_SPECIES, Float.MAX_VALUE);
            FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, -Float.MAX_VALUE);
            int count = 0;
            int limit = vector.length - BYTES_FOR_4BYTE_SPECIES.length();
            for (; i <= limit; i += FLOAT_SPECIES.length()) {
                ByteVector bv = ByteVector.fromArray(BYTES_FOR_4BYTE_SPECIES, vector, i);
                ByteVector bc = ByteVector.fromArray(BYTES_FOR_4BYTE_SPECIES, centroid, i);
                ++count;
                FloatVector v = (FloatVector) bv.castShape(FLOAT_SPECIES, 0);
                FloatVector c = (FloatVector) bc.castShape(FLOAT_SPECIES, 0);
                // centered[i] = vector[i] - centroid[i]
                FloatVector centeredVec = v.sub(c);
                // Welford online: delta = centered - mean
                FloatVector deltaVec = centeredVec.sub(vecMeanVec);
                // norm2 += centered * centered
                norm2Vec = fma(centeredVec, centeredVec, norm2Vec);
                // mean += delta / count
                vecMeanVec = vecMeanVec.add(deltaVec.mul(1f / count));
                // Welford online: m2 += delta * (centered - updatedMean)
                FloatVector delta2Vec = centeredVec.sub(vecMeanVec);
                m2Vec = fma(deltaVec, delta2Vec, m2Vec);
                // track min/max
                minVec = minVec.min(centeredVec);
                maxVec = maxVec.max(centeredVec);
                centeredVec.intoArray(centered, i);
            }
            // Reduce vector lanes to scalars
            min = minVec.reduceLanes(MIN);
            max = maxVec.reduceLanes(MAX);
            norm2 = norm2Vec.reduceLanes(ADD);
            // Parallel Welford merge across lanes: each lane tracked independent mean/m2,
            // now combine them into a single scalar mean/variance
            vecMean = vecMeanVec.reduceLanes(ADD) / FLOAT_SPECIES.length();
            FloatVector d2Mean = vecMeanVec.sub(vecMean);
            m2Vec = m2Vec.add(d2Mean.mul(d2Mean).mul(count));
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
            float alpha = (float) vectCount / totalCount;
            float beta = 1f - alpha;
            float completeMean = alpha * vecMean + beta * tailMean;
            float dMean2Lhs = (vecMean - completeMean) * (vecMean - completeMean);
            float dMean2Rhs = (tailMean - completeMean) * (tailMean - completeMean);
            vecVar = vecVar + tailM2 + vectCount * dMean2Lhs + tailCount * dMean2Rhs;
            vecMean = completeMean;
        }
        stats[0] = vecMean;
        stats[1] = vecVar / vector.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    @Override
    public void centerAndCalculateStatsDp(byte[] vector, byte[] centroid, float[] centered, float[] stats) {
        assert vector.length == centroid.length : "vector dimensions differ: " + vector.length + "!=" + centroid.length;
        assert vector.length == centered.length : "vector dimensions differ: " + vector.length + "!=" + centered.length;
        assert stats.length == 6;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        float centroidDot = 0;
        int i = 0;
        int vectCount = 0;
        if (vector.length >= BYTE_SPECIES.length()) {
            FloatVector vecMeanVec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector m2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector norm2Vec = FloatVector.zero(FLOAT_SPECIES);
            FloatVector minVec = FloatVector.broadcast(FLOAT_SPECIES, Float.MAX_VALUE);
            FloatVector maxVec = FloatVector.broadcast(FLOAT_SPECIES, -Float.MAX_VALUE);
            FloatVector centroidDotVec = FloatVector.zero(FLOAT_SPECIES);
            int count = 0;
            int limit = vector.length - BYTES_FOR_4BYTE_SPECIES.length();
            for (; i <= limit; i += FLOAT_SPECIES.length()) {
                ByteVector bv = ByteVector.fromArray(BYTES_FOR_4BYTE_SPECIES, vector, i);
                ByteVector bc = ByteVector.fromArray(BYTES_FOR_4BYTE_SPECIES, centroid, i);
                ++count;
                FloatVector v = (FloatVector) bv.castShape(FLOAT_SPECIES, 0);
                FloatVector c = (FloatVector) bc.castShape(FLOAT_SPECIES, 0);
                // centroidDot += vector[i] * centroid[i]
                centroidDotVec = fma(v, c, centroidDotVec);
                // centered[i] = vector[i] - centroid[i]
                FloatVector centeredVec = v.sub(c);
                // Welford online: delta = centered - mean
                FloatVector deltaVec = centeredVec.sub(vecMeanVec);
                // norm2 += centered * centered
                norm2Vec = fma(centeredVec, centeredVec, norm2Vec);
                // mean += delta / count
                vecMeanVec = vecMeanVec.add(deltaVec.mul(1f / count));
                // Welford online: m2 += delta * (centered - updatedMean)
                FloatVector delta2Vec = centeredVec.sub(vecMeanVec);
                m2Vec = fma(deltaVec, delta2Vec, m2Vec);
                // track min/max
                minVec = minVec.min(centeredVec);
                maxVec = maxVec.max(centeredVec);
                centeredVec.intoArray(centered, i);
            }
            // Reduce vector lanes to scalars
            min = minVec.reduceLanes(MIN);
            max = maxVec.reduceLanes(MAX);
            norm2 = norm2Vec.reduceLanes(ADD);
            centroidDot = centroidDotVec.reduceLanes(ADD);
            // Parallel Welford merge across lanes: each lane tracked independent mean/m2,
            // now combine them into a single scalar mean/variance
            vecMean = vecMeanVec.reduceLanes(ADD) / FLOAT_SPECIES.length();
            FloatVector d2Mean = vecMeanVec.sub(vecMean);
            m2Vec = m2Vec.add(d2Mean.mul(d2Mean).mul(count));
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
            float alpha = (float) vectCount / totalCount;
            float beta = 1f - alpha;
            float completeMean = alpha * vecMean + beta * tailMean;
            float dMean2Lhs = (vecMean - completeMean) * (vecMean - completeMean);
            float dMean2Rhs = (tailMean - completeMean) * (tailMean - completeMean);
            vecVar = vecVar + tailM2 + vectCount * dMean2Lhs + tailCount * dMean2Rhs;
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
    public void calculateGridPoints(float[] target, int[] quantize, int points, float[] pts) {
        assert target.length <= quantize.length;
        assert pts.length == 5;
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
    public float calculateLoss(
        float[] target,
        float lowerInterval,
        float upperInterval,
        int points,
        float norm2,
        float lambda,
        int[] quantize
    ) {
        assert upperInterval >= lowerInterval
            : "upperInterval must be greater than or equal to lowerInterval, but was: " + upperInterval + " < " + lowerInterval;
        float a = lowerInterval;
        float b = upperInterval;
        float step = ((b - a) / (points - 1.0F));
        float invStep = 1f / step;
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
            quantize[i] = Math.round((Math.clamp(target[i], a, b) - a) * invStep);
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
    public int quantizeWithIntervals(float[] vector, int[] destination, float lowInterval, float upperInterval, byte bits) {
        assert vector.length <= destination.length : "vector dimensions differ: " + vector.length + ">" + destination.length;
        assert bits > 0 && bits <= Byte.SIZE : "bits must be between 1 and 8, but was: " + bits;

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
            float xi = Math.clamp(vector[i], lowInterval, upperInterval);
            int assignment = Math.round((xi - lowInterval) * invStep);
            sumQuery += assignment;
            destination[i] = assignment;
        }
        return sumQuery;
    }

}
