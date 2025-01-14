/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;

class OptimizedScalarQuantizer {
    // The initial interval is set to the minimum MSE grid for each number of bits
    // these starting points are derived from the optimal MSE grid for a uniform distribution
    static final float[][] MINIMUM_MSE_GRID = new float[][] {
        { -0.798f, 0.798f },
        { -1.493f, 1.493f },
        { -2.051f, 2.051f },
        { -2.514f, 2.514f },
        { -2.916f, 2.916f },
        { -3.278f, 3.278f },
        { -3.611f, 3.611f },
        { -3.922f, 3.922f } };
    private static final float DEFAULT_LAMBDA = 0.1f;
    private static final int DEFAULT_ITERS = 5;
    private final VectorSimilarityFunction similarityFunction;
    private final float lambda;
    private final int iters;

    OptimizedScalarQuantizer(VectorSimilarityFunction similarityFunction, float lambda, int iters) {
        this.similarityFunction = similarityFunction;
        this.lambda = lambda;
        this.iters = iters;
    }

    OptimizedScalarQuantizer(VectorSimilarityFunction similarityFunction) {
        this(similarityFunction, DEFAULT_LAMBDA, DEFAULT_ITERS);
    }

    public record QuantizationResult(float lowerInterval, float upperInterval, float additionalCorrection, int quantizedComponentSum) {}

    public QuantizationResult[] multiScalarQuantize(float[] vector, byte[][] destinations, byte[] bits, float[] centroid) {
        assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
        assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
        assert bits.length == destinations.length;
        float[] intervalScratch = new float[2];
        double vecMean = 0;
        double vecVar = 0;
        float norm2 = 0;
        float centroidDot = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < vector.length; ++i) {
            if (similarityFunction != EUCLIDEAN) {
                centroidDot += vector[i] * centroid[i];
            }
            vector[i] = vector[i] - centroid[i];
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            norm2 += (vector[i] * vector[i]);
            double delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            vecVar += delta * (vector[i] - vecMean);
        }
        vecVar /= vector.length;
        double vecStd = Math.sqrt(vecVar);
        QuantizationResult[] results = new QuantizationResult[bits.length];
        for (int i = 0; i < bits.length; ++i) {
            assert bits[i] > 0 && bits[i] <= 8;
            int points = (1 << bits[i]);
            // Linearly scale the interval to the standard deviation of the vector, ensuring we are within the min/max bounds
            intervalScratch[0] = (float) clamp((MINIMUM_MSE_GRID[bits[i] - 1][0] + vecMean) * vecStd, min, max);
            intervalScratch[1] = (float) clamp((MINIMUM_MSE_GRID[bits[i] - 1][1] + vecMean) * vecStd, min, max);
            optimizeIntervals(intervalScratch, vector, norm2, points);
            float nSteps = ((1 << bits[i]) - 1);
            float a = intervalScratch[0];
            float b = intervalScratch[1];
            float step = (b - a) / nSteps;
            int sumQuery = 0;
            // Now we have the optimized intervals, quantize the vector
            for (int h = 0; h < vector.length; h++) {
                float xi = (float) clamp(vector[h], a, b);
                int assignment = Math.round((xi - a) / step);
                sumQuery += assignment;
                destinations[i][h] = (byte) assignment;
            }
            results[i] = new QuantizationResult(
                intervalScratch[0],
                intervalScratch[1],
                similarityFunction == EUCLIDEAN ? norm2 : centroidDot,
                sumQuery
            );
        }
        return results;
    }

    public QuantizationResult scalarQuantize(float[] vector, byte[] destination, byte bits, float[] centroid) {
        assert similarityFunction != COSINE || VectorUtil.isUnitVector(vector);
        assert similarityFunction != COSINE || VectorUtil.isUnitVector(centroid);
        assert vector.length <= destination.length;
        assert bits > 0 && bits <= 8;
        float[] intervalScratch = new float[2];
        int points = 1 << bits;
        double vecMean = 0;
        double vecVar = 0;
        float norm2 = 0;
        float centroidDot = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < vector.length; ++i) {
            if (similarityFunction != EUCLIDEAN) {
                centroidDot += vector[i] * centroid[i];
            }
            vector[i] = vector[i] - centroid[i];
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            norm2 += (vector[i] * vector[i]);
            double delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            vecVar += delta * (vector[i] - vecMean);
        }
        vecVar /= vector.length;
        double vecStd = Math.sqrt(vecVar);
        // Linearly scale the interval to the standard deviation of the vector, ensuring we are within the min/max bounds
        intervalScratch[0] = (float) clamp((MINIMUM_MSE_GRID[bits - 1][0] + vecMean) * vecStd, min, max);
        intervalScratch[1] = (float) clamp((MINIMUM_MSE_GRID[bits - 1][1] + vecMean) * vecStd, min, max);
        optimizeIntervals(intervalScratch, vector, norm2, points);
        float nSteps = ((1 << bits) - 1);
        // Now we have the optimized intervals, quantize the vector
        float a = intervalScratch[0];
        float b = intervalScratch[1];
        float step = (b - a) / nSteps;
        int sumQuery = 0;
        for (int h = 0; h < vector.length; h++) {
            float xi = (float) clamp(vector[h], a, b);
            int assignment = Math.round((xi - a) / step);
            sumQuery += assignment;
            destination[h] = (byte) assignment;
        }
        return new QuantizationResult(
            intervalScratch[0],
            intervalScratch[1],
            similarityFunction == EUCLIDEAN ? norm2 : centroidDot,
            sumQuery
        );
    }

    /**
     * Compute the loss of the vector given the interval. Effectively, we are computing the MSE of a dequantized vector with the raw
     * vector.
     * @param vector raw vector
     * @param interval interval to quantize the vector
     * @param points number of quantization points
     * @param norm2 squared norm of the vector
     * @return the loss
     */
    private double loss(float[] vector, float[] interval, int points, float norm2) {
        double a = interval[0];
        double b = interval[1];
        double step = ((b - a) / (points - 1.0F));
        double stepInv = 1.0 / step;
        double xe = 0.0;
        double e = 0.0;
        for (double xi : vector) {
            // this is quantizing and then dequantizing the vector
            double xiq = (a + step * Math.round((clamp(xi, a, b) - a) * stepInv));
            // how much does the de-quantized value differ from the original value
            xe += xi * (xi - xiq);
            e += (xi - xiq) * (xi - xiq);
        }
        return (1.0 - lambda) * xe * xe / norm2 + lambda * e;
    }

    /**
     * Optimize the quantization interval for the given vector. This is done via a coordinate descent trying to minimize the quantization
     * loss. Note, the loss is not always guaranteed to decrease, so we have a maximum number of iterations and will exit early if the
     * loss increases.
     * @param initInterval initial interval, the optimized interval will be stored here
     * @param vector raw vector
     * @param norm2 squared norm of the vector
     * @param points number of quantization points
     */
    private void optimizeIntervals(float[] initInterval, float[] vector, float norm2, int points) {
        double initialLoss = loss(vector, initInterval, points, norm2);
        final float scale = (1.0f - lambda) / norm2;
        if (Float.isFinite(scale) == false) {
            return;
        }
        for (int i = 0; i < iters; ++i) {
            float a = initInterval[0];
            float b = initInterval[1];
            float stepInv = (points - 1.0f) / (b - a);
            // calculate the grid points for coordinate descent
            double daa = 0;
            double dab = 0;
            double dbb = 0;
            double dax = 0;
            double dbx = 0;
            for (float xi : vector) {
                float k = Math.round((clamp(xi, a, b) - a) * stepInv);
                float s = k / (points - 1);
                daa += (1.0 - s) * (1.0 - s);
                dab += (1.0 - s) * s;
                dbb += s * s;
                dax += xi * (1.0 - s);
                dbx += xi * s;
            }
            double m0 = scale * dax * dax + lambda * daa;
            double m1 = scale * dax * dbx + lambda * dab;
            double m2 = scale * dbx * dbx + lambda * dbb;
            // its possible that the determinant is 0, in which case we can't update the interval
            double det = m0 * m2 - m1 * m1;
            if (det == 0) {
                return;
            }
            float aOpt = (float) ((m2 * dax - m1 * dbx) / det);
            float bOpt = (float) ((m0 * dbx - m1 * dax) / det);
            // If there is no change in the interval, we can stop
            if ((Math.abs(initInterval[0] - aOpt) < 1e-8 && Math.abs(initInterval[1] - bOpt) < 1e-8)) {
                return;
            }
            double newLoss = loss(vector, new float[] { aOpt, bOpt }, points, norm2);
            // If the new loss is worse, don't update the interval and exit
            // This optimization, unlike kMeans, does not always converge to better loss
            // So exit if we are getting worse
            if (newLoss > initialLoss) {
                return;
            }
            // Update the interval and go again
            initInterval[0] = aOpt;
            initInterval[1] = bOpt;
            initialLoss = newLoss;
        }
    }

    private static double clamp(double x, double a, double b) {
        return Math.min(Math.max(x, a), b);
    }

}
