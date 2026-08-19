/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import static java.lang.Math.fma;

/**
 * The per-vector kernels used to optimized-scalar quantize a single vector against a centroid.
 * <p>
 * This class holds the portable scalar implementations; subclasses replace individual
 * kernels with SIMD versions. Callers must not instantiate it directly, and instead obtain
 * the implementation appropriate for the current runtime from
 * {@link VectorScorerFactory#newOptimizedScalarQuantization()}.
 * <p>
 * Implementations hold no state, so a single instance can be shared across threads.
 */
public class OptimizedScalarQuantization {

    /**
     * Calculate the loss for optimized-scalar quantization for the given parameters
     * @param target The vector being quantized, assumed to be centered
     * @param lowerInterval The lower interval value for which to calculate the loss
     * @param upperInterval The upper interval value for which to calculate the loss
     * @param points the quantization points
     * @param norm2 The norm squared of the target vector
     * @param lambda The lambda parameter for controlling anisotropic loss calculation
     * @param quantize array to store the computed quantize vector.
     *
     * @return The loss for the given parameters
     */
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
        for (int i = 0; i < target.length; ++i) {
            float xi = target[i];
            // this is quantizing and then dequantizing the vector
            quantize[i] = Math.round((Math.min(Math.max(xi, a), b) - a) * invStep);
            float xiq = fma(step, quantize[i], a);
            // how much does the de-quantized value differ from the original value
            float xiiq = xi - xiq;
            e = fma(xiiq, xiiq, e);
            xe = fma(xi, xiiq, xe);
        }
        return (1f - lambda) * xe * xe / norm2 + lambda * e;
    }

    /**
     * Calculate the grid points for optimized-scalar quantization
     * @param target The vector being quantized, assumed to be centered
     * @param quantize The quantize vector which should have at least the target vector length
     * @param points the quantization points
     * @param pts The array to store the grid points, must be of length 5
     */
    public void calculateGridPoints(float[] target, int[] quantize, int points, float[] pts) {
        assert target.length <= quantize.length;
        assert pts.length == 5;
        float daa = 0;
        float dab = 0;
        float dbb = 0;
        float dax = 0;
        float dbx = 0;
        float invPmOnes = 1f / (points - 1f);
        for (int i = 0; i < target.length; ++i) {
            float v = target[i];
            float k = quantize[i];
            float s = k * invPmOnes;
            float ms = 1f - s;
            daa = fma(ms, ms, daa);
            dab = fma(ms, s, dab);
            dbb = fma(s, s, dbb);
            dax = fma(ms, v, dax);
            dbx = fma(s, v, dbx);
        }
        pts[0] = daa;
        pts[1] = dab;
        pts[2] = dbb;
        pts[3] = dax;
        pts[4] = dbx;
    }

    /**
     * Center the target vector and calculate the optimized-scalar quantization statistics
     * @param target The vector being quantized
     * @param centroid The centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 5
     */
    public void centerAndCalculateStatsEuclidean(float[] target, float[] centroid, float[] centered, float[] stats) {
        assert target.length == centroid.length : "vector dimensions differ: " + target.length + "!=" + centroid.length;
        assert target.length == centered.length : "vector dimensions differ: " + target.length + "!=" + centered.length;
        assert stats.length == 5;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            centered[i] = target[i] - centroid[i];
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    /**
     * Center the byte target vector against a byte centroid and calculate the optimized-scalar quantization statistics
     * for euclidean similarity.
     * @param target The byte vector being quantized
     * @param centroid The byte centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 5
     */
    public void centerAndCalculateStatsEuclidean(byte[] target, byte[] centroid, float[] centered, float[] stats) {
        assert target.length == centroid.length : "vector dimensions differ: " + target.length + "!=" + centroid.length;
        assert target.length == centered.length : "vector dimensions differ: " + target.length + "!=" + centered.length;
        assert stats.length == 5;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            centered[i] = (float) (target[i] - centroid[i]);
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    /**
     * Center the target vector and calculate the optimized-scalar quantization statistics
     * @param target The vector being quantized
     * @param centroid The centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 6
     */
    public void centerAndCalculateStatsDp(float[] target, float[] centroid, float[] centered, float[] stats) {
        assert target.length == centroid.length : "vector dimensions differ: " + target.length + "!=" + centroid.length;
        assert target.length == centered.length : "vector dimensions differ: " + target.length + "!=" + centered.length;
        assert stats.length == 6;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float centroidDot = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            centroidDot = fma(target[i], centroid[i], centroidDot);
            centered[i] = target[i] - centroid[i];
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
        stats[5] = centroidDot;
    }

    /**
     * Center the byte target vector against a byte centroid and calculate the optimized-scalar quantization statistics
     * for dot-product similarity.
     * @param target The byte vector being quantized
     * @param centroid The byte centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 6
     */
    public void centerAndCalculateStatsDp(byte[] target, byte[] centroid, float[] centered, float[] stats) {
        assert target.length == centroid.length : "vector dimensions differ: " + target.length + "!=" + centroid.length;
        assert target.length == centered.length : "vector dimensions differ: " + target.length + "!=" + centered.length;
        assert stats.length == 6;

        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float centroidDot = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            float t = target[i];
            float c = centroid[i];
            centroidDot = fma(t, c, centroidDot);
            centered[i] = (float) (target[i] - centroid[i]);
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
        stats[5] = centroidDot;
    }

    /**
     * Optimized-scalar quantization of the provided vector to the provided destination array.
     *
     * @param vector the vector to quantize
     * @param destination the array to store the result
     * @param lowInterval the minimum value, lower values in the original array will be replaced by this value
     * @param upperInterval the maximum value, bigger values in the original array will be replaced by this value
     * @param bits the number of bits to use for quantization, must be between 1 and 8
     *
     * @return return the sum of all the elements of the resulting quantized vector.
     */
    public int quantizeWithIntervals(float[] vector, int[] destination, float lowInterval, float upperInterval, byte bits) {
        assert vector.length == destination.length : "vector dimensions differ: " + vector.length + "!=" + destination.length;
        assert bits > 0 && bits <= Byte.SIZE : "bits must be between 1 and 8, but was: " + bits;

        float nSteps = ((1 << bits) - 1);
        float invStep = nSteps / (upperInterval - lowInterval);
        int sumQuery = 0;
        for (int h = 0; h < vector.length; h++) {
            float xi = Math.min(Math.max(vector[h], lowInterval), upperInterval);
            int assignment = Math.round((xi - lowInterval) * invStep);
            sumQuery += assignment;
            destination[h] = assignment;
        }
        return sumQuery;
    }
}
