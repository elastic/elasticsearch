/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.function.DoubleUnaryOperator;

/**
 * Computes expected recall@k for a given quantization scheme using manifold model parameters,
 * Gaussian error assumptions, and Gauss-Legendre quadrature.
 */
public final class ExpectedRecall {

    /**
     * Reused across calls on the same thread to avoid allocating {@code 20 * n} doubles per
     * {@link #expectedRecallAtK} invocation; grows if a larger calibration asks for bigger {@code n}.
     */
    private static final ThreadLocal<double[]> RANK_DISTANCE_SCRATCH = new ThreadLocal<>();

    private static final double SQRT_2_PI = Math.sqrt(2.0 * Math.PI);

    private static final double[] QUADRATURE_NODES = {
        -0.906179845938664,
        -0.5384693101056831,
        0.0,
        0.5384693101056831,
        0.906179845938664 };

    private static final double[] QUADRATURE_WEIGHTS = {
        0.2369268850561891,
        0.4786286704993665,
        0.5688888888888889,
        0.4786286704993665,
        0.2369268850561891 };

    private ExpectedRecall() {}

    /**
     * Expected recall@k when reranking top-n candidates: average of expected recall for ranks 1..k.
     *
     * @param similarityFunction the vector similarity metric
     * @param N                  total number of documents in the corpus
     * @param alpha              manifold model intercept
     * @param invDim             manifold model slope
     * @param errorStd           quantization error standard deviation
     * @param k                  number of results to return
     * @param n                  number of candidates to rerank
     * @return expected recall in [0, 1]
     */
    public static double expectedRecallAtK(
        VectorSimilarityFunction similarityFunction,
        int N,
        double alpha,
        double invDim,
        double errorStd,
        int k,
        int n
    ) {
        int maxRank = 20 * n;
        double[] rankDistances = borrowRankDistancesScratch(maxRank);
        for (int i = 0; i < maxRank; i++) {
            rankDistances[i] = ManifoldModel.expectedRankDistance(similarityFunction, alpha, invDim, N, i + 1);
        }

        double total = 0;
        for (int i = 1; i <= k; i++) {
            total += expectedRecallOfK(rankDistances, errorStd, n, i);
        }
        return total / k;
    }

    private static double[] borrowRankDistancesScratch(int minLength) {
        double[] buf = RANK_DISTANCE_SCRATCH.get();
        if (buf == null || buf.length < minLength) {
            buf = new double[minLength];
            RANK_DISTANCE_SCRATCH.set(buf);
        }
        return buf;
    }

    /**
     * Rerank count from fraction: (k * num + den/2) / den.
     */
    public static int rerankN(int k, int num, int den) {
        return (k * num + den / 2) / den;
    }

    static double expectedRecallOfK(double[] rankDistances, double errorStd, int n, int k) {
        double mk = rankDistances[k - 1];
        double integral = quadrature(
            x -> probabilityRankLessThanN(rankDistances, errorStd, n, x) * normalPdf(x, mk, errorStd),
            mk - 9.0 * errorStd,
            mk + 9.0 * errorStd,
            6
        );
        return Math.min(integral, 1.0);
    }

    static double probabilityRankLessThanN(double[] rankDistances, double errorStd, int n, double x) {
        double mu = 0, stddevSq = 0;
        int limit = Math.min(20 * n, rankDistances.length);
        for (int i = 0; i < limit; i++) {
            double fx = normalCdf(x, rankDistances[i], errorStd);
            mu += fx;
            stddevSq += fx * fx;
        }
        double stddev = Math.sqrt(mu - stddevSq);
        return normalCdf(n, mu, stddev);
    }

    static double normalPdf(double x, double mean, double stddev) {
        return Math.exp(-0.5 * Math.pow((x - mean) / stddev, 2)) / (stddev * SQRT_2_PI);
    }

    static double normalCdf(double x, double mean, double stddev) {
        return 0.5 * (1.0 + erf((x - mean) / (stddev * Math.sqrt(2.0))));
    }

    static double erf(double z) {
        double t = 1.0 / (1.0 + 0.5 * Math.abs(z));
        double ans = 1 - t * Math.exp(
            -z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t
                * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))
        );
        return z >= 0 ? ans : -ans;
    }

    static double quadrature(DoubleUnaryOperator f, double a, double b, int n) {
        double intervalLength = (b - a) / n;
        double result = 0;
        for (int i = 0; i < n; i++) {
            double start = a + i * intervalLength;
            double integral = 0;
            for (int j = 0; j < 5; j++) {
                double x = start + 0.5 * intervalLength * (QUADRATURE_NODES[j] + 1.0);
                integral += QUADRATURE_WEIGHTS[j] * f.applyAsDouble(x);
            }
            result += 0.5 * intervalLength * integral;
        }
        return result;
    }
}
