/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.util.Iterator;

/**
 * Static class with methods for comparing distributions.
 */
@SuppressWarnings("WeakerAccess")
public class Comparison {

    /**
     * Use a log-likelihood ratio test to compare two distributions.
     * This is done by estimating counts in quantile ranges from each
     * distribution and then comparing those counts using a multinomial
     * test. The result should be asymptotically chi^2 distributed if
     * the data comes from the same distribution, but this isn't so
     * much useful as a traditional test of a null hypothesis as it is
     * just a reasonably well-behaved score that is bigger when the
     * distributions are more different, subject to having enough data
     * to tell.
     *
     * @param dist1 First distribution (usually the reference)
     * @param dist2 Second distribution to compare (usually the test case)
     * @param qCuts The quantiles that define the bin boundaries. Values &le;0 or &ge;1
     *              may result in zero counts. Note that the actual cuts are
     *              defined loosely as <pre>dist1.quantile(qCuts[i])</pre>.
     * @return A score that is big when dist1 and dist2 are discernibly different.
     * A small score does not mean similarity. Instead, it could just mean insufficient
     * data.
     */
    @SuppressWarnings("WeakerAccess")
    public static double compareChi2(TDigest dist1, TDigest dist2, double[] qCuts) {
        double[][] count = new double[2][];
        count[0] = new double[qCuts.length + 1];
        count[1] = new double[qCuts.length + 1];

        double oldQ = 0;
        double oldQ2 = 0;
        for (int i = 0; i <= qCuts.length; i++) {
            double newQ;
            double x;
            if (i == qCuts.length) {
                newQ = 1;
                x = Math.max(dist1.getMax(), dist2.getMax()) + 1;
            } else {
                newQ = qCuts[i];
                x = dist1.quantile(newQ);
            }
            count[0][i] = dist1.size() * (newQ - oldQ);

            double q2 = dist2.cdf(x);
            count[1][i] = dist2.size() * (q2 - oldQ2);
            oldQ = newQ;
            oldQ2 = q2;
        }

        return llr(count);
    }

    /**
     * Use a log-likelihood ratio test to compare two distributions.
     * With non-linear histograms that have compatible bin boundaries,
     * all that we have to do is compare two count vectors using a
     * chi^2 test (actually a log-likelihood ratio version called a G-test).
     *
     * @param dist1 First distribution (usually the reference)
     * @param dist2 Second distribution to compare (usually the test case)
     * @return A score that is big when dist1 and dist2 are discernibly different.
     * A small score does not mean similarity. Instead, it could just mean insufficient
     * data.
     */
    @SuppressWarnings("WeakerAccess")
    public static double compareChi2(Histogram dist1, Histogram dist2) {
        if (!dist1.getClass().equals(dist2.getClass())) {
            throw new IllegalArgumentException(String.format("Must have same class arguments, got %s and %s",
                    dist1.getClass(), dist2.getClass()));
        }

        long[] k1 = dist1.getCounts();
        long[] k2 = dist2.getCounts();

        int n1 = k1.length;
        if (n1 != k2.length ||
                dist1.lowerBound(0) != dist2.lowerBound(0) ||
                dist1.lowerBound(n1 - 1) != dist2.lowerBound(n1 - 1)) {
            throw new IllegalArgumentException("Incompatible histograms in terms of size or bounds");
        }

        double[][] count = new double[2][n1];
        for (int i = 0; i < n1; i++) {
            count[0][i] = k1[i];
            count[1][i] = k2[i];
        }
        return llr(count);
    }

    @SuppressWarnings("WeakerAccess")
    public static double llr(double[][] count) {
        if (count.length == 0) {
            throw new IllegalArgumentException("Must have some data in llr");
        }
        int columns = count[0].length;
        int rows = count.length;
        double[] rowSums = new double[rows];
        double[] colSums = new double[columns];

        double totalCount = 0;
        double h = 0; // accumulator for entropy

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                double k = count[i][j];
                rowSums[i] += k;
                colSums[j] += k;
                if (k < 0) {
                    throw new IllegalArgumentException(String.format("Illegal negative count (%.5f) at %d,%d", k, i, j));
                }
                if (k > 0) {
                    h += k * Math.log(k);
                    totalCount += k;
                }
            }
        }

        double normalizer = totalCount * Math.log(totalCount);
        h -= normalizer;  // same as dividing every count by total inside the log

        double hr = 0; // accumulator for row-wise entropy
        for (int i = 0; i < rows; i++) {
            if (rowSums[i] > 0) {
                hr += rowSums[i] * Math.log(rowSums[i]);
            }
        }
        hr -= normalizer;

        double hc = 0; // accumulator for column-wise entropy
        for (int j = 0; j < columns; j++) {
            if (colSums[j] > 0) {
                hc += colSums[j] * Math.log(colSums[j]);
            }
        }
        hc -= normalizer;
        // return value is 2N * mutualInformation(count)
        return 2 * (h - hr - hc);
    }

    /**
     * Returns the observed value of the Kolmogorov-Smirnov statistic normalized by sample counts so
     * that the score should be roughly distributed as sqrt(-log(u)/2). This is equal to the normal
     * KS statistic multiplied by sqrt(m*n/(m+n)) where m and n are the number of samples in d1 and
     * d2 respectively.
     * @param d1  A digest of the first set of samples.
     * @param d2  A digest of the second set of samples.
     * @return A statistic which is bigger when d1 and d2 seem to represent different distributions.
     */
    public static double ks(TDigest d1, TDigest d2) {
        Iterator<Centroid> ix1 = d1.centroids().iterator();
        Iterator<Centroid> ix2 = d2.centroids().iterator();

        double diff = 0;

        double x1 = d1.getMin();
        double x2 = d2.getMin();

        while (x1 <= d1.getMax() && x2 <= d2.getMax()) {
            if (x1 < x2) {
                diff = maxDiff(d1, d2, diff, x1);
                x1 = nextValue(d1, ix1, x1);
            } else if (x1 > x2) {
                diff = maxDiff(d1, d2, diff, x2);
                x2 = nextValue(d2, ix2, x2);
            } else if (x1 == x2) {
                diff = maxDiff(d1, d2, diff, x1);

                double q1 = d1.cdf(x1);
                double q2 = d2.cdf(x2);
                if (q1 < q2) {
                    x1 = nextValue(d1, ix1, x1);
                } else if (q1 > q2) {
                    x2 = nextValue(d2, ix2, x2);
                } else {
                    x1 = nextValue(d1, ix1, x1);
                    x2 = nextValue(d2, ix2, x2);
                }
            }
        }
        while (x1 <= d1.getMax()) {
            diff = maxDiff(d1, d2, diff, x1);
            x1 = nextValue(d1, ix1, x1);
        }

        while (x2 <= d2.getMax()) {
            diff = maxDiff(d2, d2, diff, x2);
            x2 = nextValue(d2, ix2, x2);
        }

        long n1 = d1.size();
        long n2 = d2.size();
        return diff * Math.sqrt((double) n1 * n2 / (n1 + n2));
    }

    private static double maxDiff(TDigest d1, TDigest d2, double diff, double x1) {
        diff = Math.max(diff, Math.abs(d1.cdf(x1) - d2.cdf(x1)));
        return diff;
    }

    private static double nextValue(TDigest d, Iterator<Centroid> ix, double x) {
        if (ix.hasNext()) {
            return ix.next().mean();
        } else if (x < d.getMax()) {
            return d.getMax();
        } else {
            return d.getMax() + 1;
        }
    }
}
