/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.function.DoubleUnaryOperator;

/**
 * Computes expected recall@k for a given quantization scheme using manifold model parameters,
 * Gaussian error assumptions, and Gauss-Legendre quadrature.
 * Rank distances are perturbed by additive Gaussian noise; expected recall is obtained by integrating over that noise model with
 * <a href="https://en.wikipedia.org/wiki/Gauss%E2%80%93Legendre_quadrature">Gauss-Legendre quadrature</a>.
 */
public final class ExpectedRecall {

    private static final double SQRT_2_PI = Math.sqrt(2.0 * Math.PI);

    /**
     * 5-point Gauss-Legendre quadrature rule on the canonical interval [-1, 1].
     * The nodes are the roots of the degree-5 Legendre polynomial {@code P5(x)}, and the
     * weights are the corresponding coefficients from the standard Gauss-Legendre
     * table. Together they integrate polynomials up to degree 9 exactly before the
     * affine mapping in {@link #quadrature} moves the rule onto each subinterval.
     * See <a href="https://en.wikipedia.org/wiki/Gauss%E2%80%93Legendre_quadrature">Gauss-Legendre quadrature</a>.
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

    private static final int RERANK_WINDOW_RANK_MULTIPLIER = 20;

    private ExpectedRecall() {}

    /**
     * Expected recall@k when reranking top-n candidates: average of expected recall for ranks 1..k.
     *
     * @param similarityFunction the vector similarity metric
     * @param numDocs            total number of documents in the corpus
     * @param manifoldIntercept  manifold model intercept
     * @param invDim             manifold model slope
     * @param errorStd           quantization error standard deviation
     * @param k                  number of results to return
     * @param n                  number of candidates to rerank
     * @return expected recall in [0, 1]
     */
    public static double expectedRecallAtK(
        VectorSimilarityFunction similarityFunction,
        int numDocs,
        double manifoldIntercept,
        double invDim,
        double errorStd,
        int k,
        int n
    ) {
        int maxRank = Math.min(RERANK_WINDOW_RANK_MULTIPLIER * n, numDocs); // at most numDocs ranks
        double[] rankDistances = new double[maxRank];
        for (int i = 0; i < maxRank; i++) {
            rankDistances[i] = ManifoldModel.expectedRankDistance(similarityFunction, manifoldIntercept, invDim, numDocs, i + 1);
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
     * Expected recall for a single result rank {@code k} within a rerank window of size {@code n}.
     * <p>
     * Let {@code m_k} be the manifold-predicted distance at rank {@code k} and {@code errorStd}
     * be {@code sigma}. This evaluates the integral of
     * {@code P(rank < n | x) * N(x; m_k, sigma^2)} over {@code [m_k - 9*sigma, m_k + 9*sigma]},
     * where {@code P(rank < n | x)} comes from {@link #probabilityRankLessThanN} and
     * {@code N} is the
     * <a href="https://en.wikipedia.org/wiki/Normal_distribution">normal</a>
     * <a href="https://en.wikipedia.org/wiki/Probability_density_function">probability density function</a>.
     * The integral is approximated by {@link #quadrature}.
     */
    static double expectedRecallOfK(double[] rankDistances, double errorStd, int windowSize, int k) {
        double mk = rankDistances[k - 1];
        double integral = quadrature(
            x -> probabilityRankLessThanN(rankDistances, errorStd, windowSize, x) * normalPdf(x, mk, errorStd),
            mk - 9.0 * errorStd,
            mk + 9.0 * errorStd,
            6
        );
        return Math.min(integral, 1.0);
    }

    /**
     * Computes the probability that a rank, drawn from a distribution of ranks with added noise,
     * is less than the specified rank threshold given a set of rank distances and an error standard deviation.
     * <p>
     * Each rank distance is treated as the mean of a
     * <a href="https://en.wikipedia.org/wiki/Normal_distribution">Gaussian</a> perturbation; the
     * corresponding <a href="https://en.wikipedia.org/wiki/Cumulative_distribution_function">cumulative distribution function</a>
     * values are aggregated, and a second normal CDF maps that aggregate to a rank probability.
     *
     * @param rankDistances an array of rank distances representing the distribution of possible ranks
     * @param errorStd the standard deviation of the error or noise added to the rank distances
     * @param rankThreshold the rank threshold to compare against
     * @param x the value at which to calculate the cumulative distribution function for individual ranks
     * @return the cumulative probability that the rank is less than the rank threshold
     */
    static double probabilityRankLessThanN(double[] rankDistances, double errorStd, int rankThreshold, double x) {
        double mu = 0, stddevSq = 0;
        int limit = Math.min(RERANK_WINDOW_RANK_MULTIPLIER * rankThreshold, rankDistances.length);
        for (int i = 0; i < limit; i++) {
            double fx = normalCdf(x, rankDistances[i], errorStd);
            mu += fx;
            stddevSq += fx * fx;
        }
        double stddev = Math.sqrt(Math.max(1e-5, mu - stddevSq)); // avoid divide-by-zero
        return normalCdf(rankThreshold, mu, stddev);
    }

    /**
     * Computes the probability density function (PDF) of a normal (Gaussian) distribution
     * with the specified mean and standard deviation at a given point.
     * See the <a href="https://en.wikipedia.org/wiki/Normal_distribution">normal distribution</a>
     * and <a href="https://en.wikipedia.org/wiki/Probability_density_function">probability density function</a>.
     *
     * @param x the point at which to evaluate the normal distribution
     * @param mean the mean (expected value) of the normal distribution
     * @param stddev the standard deviation (spread) of the normal distribution
     * @return the value of the normal distribution PDF at the given point
     */
    static double normalPdf(double x, double mean, double stddev) {
        assert stddev > 0 : "stddev must be positive, got " + stddev;
        double z = (x - mean) / stddev;
        return Math.exp(-0.5 * z * z) / (stddev * SQRT_2_PI);
    }

    /**
     * Computes the cumulative distribution function (CDF) of a normal (Gaussian)
     * distribution with the specified mean and standard deviation at a given point.
     * Uses the <a href="https://en.wikipedia.org/wiki/Error_function">error function</a>
     * identity for the
     * <a href="https://en.wikipedia.org/wiki/Normal_distribution">normal</a>
     * <a href="https://en.wikipedia.org/wiki/Cumulative_distribution_function">CDF</a>.
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
     * See the <a href="https://en.wikipedia.org/wiki/Error_function">error function</a>.
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
     * Each subinterval applies the
     * <a href="https://en.wikipedia.org/wiki/Gauss%E2%80%93Legendre_quadrature">Gauss-Legendre quadrature</a>
     * rule from {@link #QUADRATURE_NODES} and {@link #QUADRATURE_WEIGHTS}; see also
     * <a href="https://en.wikipedia.org/wiki/Numerical_integration">numerical integration</a>.
     *
     * @param f the mathematical function to be integrated, represented as a DoubleUnaryOperator
     * @param lb the lower bound of the integration interval
     * @param ub the upper bound of the integration interval
     * @param subIntervals the number of subintervals to divide the integration interval into
     * @return an approximation of the integral of the function f over the interval [a, b]
     */
    static double quadrature(DoubleUnaryOperator f, double lb, double ub, int subIntervals) {
        double intervalLength = (ub - lb) / subIntervals;
        double result = 0;
        for (int i = 0; i < subIntervals; i++) {
            double start = lb + i * intervalLength;
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
