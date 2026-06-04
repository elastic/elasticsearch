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

    private static final double SQRT_2_PI = Math.sqrt(2.0 * Math.PI);

    /**
     * 5-point Gauss-Legendre quadrature rule on the canonical interval [-1, 1].
     * The nodes are the roots of the degree-5 Legendre polynomial {@code P5(x)}, and the
     * weights are the corresponding coefficients from the standard Gauss-Legendre
     * table. Together they integrate polynomials up to degree 9 exactly before the
     * affine mapping in {@link #quadrature} moves the rule onto each subinterval.
     */
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
        double[] rankDistances = new double[maxRank];
        for (int i = 0; i < maxRank; i++) {
            rankDistances[i] = ManifoldModel.expectedRankDistance(similarityFunction, alpha, invDim, N, i + 1);
        }

        double total = 0;
        for (int i = 1; i <= k; i++) {
            total += expectedRecallOfK(rankDistances, errorStd, n, i);
        }
        return total / k;
    }

    /**
     * Rerank count from continuous depth multiplier: {@code round(depth * k)}.
     */
    public static int rerankN(int k, double rerankDepth) {
        return Math.max(1, (int) Math.round(rerankDepth * k));
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

    /**
     * Computes the probability that a rank, drawn from a distribution of ranks with added noise,
     * is less than the specified rank threshold n given a set of rank distances and an error standard deviation.
     *
     * @param rankDistances an array of rank distances representing the distribution of possible ranks
     * @param errorStd the standard deviation of the error or noise added to the rank distances
     * @param n the rank threshold to compare against
     * @param x the value at which to calculate the cumulative distribution function for individual ranks
     * @return the cumulative probability that the rank is less than n
     */
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

    /**
     * Computes the probability density function (PDF) of a normal (Gaussian) distribution
     * with the specified mean and standard deviation at a given point.
     *
     * @param x the point at which to evaluate the normal distribution
     * @param mean the mean (expected value) of the normal distribution
     * @param stddev the standard deviation (spread) of the normal distribution
     * @return the value of the normal distribution PDF at the given point
     */
    static double normalPdf(double x, double mean, double stddev) {
        return Math.exp(-0.5 * Math.pow((x - mean) / stddev, 2)) / (stddev * SQRT_2_PI);
    }

    /**
     * Computes the cumulative distribution function (CDF) of a normal (Gaussian)
     * distribution with the specified mean and standard deviation at a given point.
     *
     * @param x the point at which to evaluate the normal distribution
     * @param mean the mean (expected value) of the normal distribution
     * @param stddev the standard deviation (spread) of the normal distribution
     * @return the cumulative probability corresponding to the input point, representing
     *         the area under the normal distribution curve to the left of the input point
     */
    static double normalCdf(double x, double mean, double stddev) {
        return 0.5 * (1.0 + erf((x - mean) / (stddev * Math.sqrt(2.0))));
    }

    /**
     * Returns an approximation of the Gauss error function {@code erf(z)}.
     * The error function is defined as:
     * {@code erf(z) = 2 / sqrt(pi) * integral(exp(-x * x), x = 0..z)}.
     * This implementation uses a polynomial approximation evaluated with
     * Horner's method and preserves the odd symmetry {@code erf(-z) = -erf(z)}.
     *
     * @param z the input value
     * @return an approximate value of {@code erf(z)}
     */
    static double erf(double z) {
        double t = 1.0 / (1.0 + 0.5 * Math.abs(z));
        double ans = 1 - t * Math.exp(
            -z * z - 1.26551223 + t * (1.00002368 + t * (0.37409196 + t * (0.09678418 + t * (-0.18628806 + t * (0.27886807 + t
                * (-1.13520398 + t * (1.48851587 + t * (-0.82215223 + t * 0.17087277))))))))
        );
        return z >= 0 ? ans : -ans;
    }

    /**
     * Approximates the definite integral of a given function over the interval [a, b]
     * using numerical quadrature with a specified number of subintervals.
     *
     * @param f the mathematical function to be integrated, represented as a DoubleUnaryOperator
     * @param a the lower bound of the integration interval
     * @param b the upper bound of the integration interval
     * @param n the number of subintervals to divide the integration interval into
     * @return an approximation of the integral of the function f over the interval [a, b]
     */
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
