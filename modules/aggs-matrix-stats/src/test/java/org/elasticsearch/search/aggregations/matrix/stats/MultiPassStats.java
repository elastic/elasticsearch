/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.util.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class MultiPassStats {

    private final String fieldAKey;
    private final String fieldBKey;

    private long count;
    private Map<String, Double> means = new HashMap<>();
    private Map<String, Double> variances = new HashMap<>();
    private Map<String, Double> skewness = new HashMap<>();
    private Map<String, Double> kurtosis = new HashMap<>();
    private Map<String, Map<String, Double>> covariances = new HashMap<>();
    private Map<String, Map<String, Double>> correlations = new HashMap<>();

    MultiPassStats(String fieldAName, String fieldBName) {
        this.fieldAKey = fieldAName;
        this.fieldBKey = fieldBName;
    }

    void computeStats(final List<Double> fieldA, final List<Double> fieldB) {
        // set count
        count = fieldA.size();
        double meanA = 0d;
        double meanB = 0d;

        // compute mean
        for (int n = 0; n < count; ++n) {
            // fieldA
            meanA += fieldA.get(n);
            meanB += fieldB.get(n);
        }
        means.put(fieldAKey, meanA / count);
        means.put(fieldBKey, meanB / count);

        // compute variance, skewness, and kurtosis
        double dA;
        double dB;
        double skewA = 0d;
        double skewB = 0d;
        double kurtA = 0d;
        double kurtB = 0d;
        double varA = 0d;
        double varB = 0d;
        double cVar = 0d;
        for (int n = 0; n < count; ++n) {
            dA = fieldA.get(n) - means.get(fieldAKey);
            varA += dA * dA;
            skewA += dA * dA * dA;
            kurtA += dA * dA * dA * dA;
            dB = fieldB.get(n) - means.get(fieldBKey);
            varB += dB * dB;
            skewB += dB * dB * dB;
            kurtB += dB * dB * dB * dB;
            cVar += dA * dB;
        }
        variances.put(fieldAKey, varA / (count - 1));
        final double stdA = Math.sqrt(variances.get(fieldAKey));
        variances.put(fieldBKey, varB / (count - 1));
        final double stdB = Math.sqrt(variances.get(fieldBKey));
        skewness.put(fieldAKey, skewA / ((count - 1) * variances.get(fieldAKey) * stdA));
        skewness.put(fieldBKey, skewB / ((count - 1) * variances.get(fieldBKey) * stdB));
        kurtosis.put(fieldAKey, kurtA / ((count - 1) * variances.get(fieldAKey) * variances.get(fieldAKey)));
        kurtosis.put(fieldBKey, kurtB / ((count - 1) * variances.get(fieldBKey) * variances.get(fieldBKey)));

        // compute covariance
        final Map<String, Double> fieldACovar = Maps.newMapWithExpectedSize(2);
        fieldACovar.put(fieldAKey, 1d);
        cVar /= count - 1;
        fieldACovar.put(fieldBKey, cVar);
        covariances.put(fieldAKey, fieldACovar);
        final Map<String, Double> fieldBCovar = Maps.newMapWithExpectedSize(2);
        fieldBCovar.put(fieldAKey, cVar);
        fieldBCovar.put(fieldBKey, 1d);
        covariances.put(fieldBKey, fieldBCovar);

        // compute correlation
        final HashMap<String, Double> fieldACorr = new HashMap<>();
        fieldACorr.put(fieldAKey, 1d);
        double corr = covariances.get(fieldAKey).get(fieldBKey);
        corr /= stdA * stdB;
        fieldACorr.put(fieldBKey, corr);
        correlations.put(fieldAKey, fieldACorr);
        final HashMap<String, Double> fieldBCorr = new HashMap<>();
        fieldBCorr.put(fieldAKey, corr);
        fieldBCorr.put(fieldBKey, 1d);
        correlations.put(fieldBKey, fieldBCorr);
    }

    void assertNearlyEqual(MatrixStatsResults stats) {
        assertEquals(count, stats.getDocCount());
        assertEquals(count, stats.getFieldCount(fieldAKey));
        assertEquals(count, stats.getFieldCount(fieldBKey));
        // means
        assertTrue(nearlyEqual(means.get(fieldAKey), stats.getMean(fieldAKey), 1e-7));
        assertTrue(nearlyEqual(means.get(fieldBKey), stats.getMean(fieldBKey), 1e-7));
        // variances
        assertTrue(nearlyEqual(variances.get(fieldAKey), stats.getVariance(fieldAKey), 1e-7));
        assertTrue(nearlyEqual(variances.get(fieldBKey), stats.getVariance(fieldBKey), 1e-7));
        // skewness (multi-pass is more susceptible to round-off error so we need to slightly relax the tolerance)
        assertTrue(nearlyEqual(skewness.get(fieldAKey), stats.getSkewness(fieldAKey), 1e-4));
        assertTrue(nearlyEqual(skewness.get(fieldBKey), stats.getSkewness(fieldBKey), 1e-4));
        // kurtosis (multi-pass is more susceptible to round-off error so we need to slightly relax the tolerance)
        assertTrue(nearlyEqual(kurtosis.get(fieldAKey), stats.getKurtosis(fieldAKey), 1e-4));
        assertTrue(nearlyEqual(kurtosis.get(fieldBKey), stats.getKurtosis(fieldBKey), 1e-4));
        // covariances
        assertTrue(nearlyEqual(covariances.get(fieldAKey).get(fieldBKey), stats.getCovariance(fieldAKey, fieldBKey), 1e-7));
        assertTrue(nearlyEqual(covariances.get(fieldBKey).get(fieldAKey), stats.getCovariance(fieldBKey, fieldAKey), 1e-7));
        // correlation
        assertTrue(nearlyEqual(correlations.get(fieldAKey).get(fieldBKey), stats.getCorrelation(fieldAKey, fieldBKey), 1e-7));
        assertTrue(nearlyEqual(correlations.get(fieldBKey).get(fieldAKey), stats.getCorrelation(fieldBKey, fieldAKey), 1e-7));
    }

    void assertNearlyEqual(InternalMatrixStats stats) {
        assertEquals(count, stats.getDocCount());
        assertEquals(count, stats.getFieldCount(fieldAKey));
        assertEquals(count, stats.getFieldCount(fieldBKey));
        // means
        assertTrue(nearlyEqual(means.get(fieldAKey), stats.getMean(fieldAKey), 1e-7));
        assertTrue(nearlyEqual(means.get(fieldBKey), stats.getMean(fieldBKey), 1e-7));
        // variances
        assertTrue(nearlyEqual(variances.get(fieldAKey), stats.getVariance(fieldAKey), 1e-7));
        assertTrue(nearlyEqual(variances.get(fieldBKey), stats.getVariance(fieldBKey), 1e-7));
        // skewness (multi-pass is more susceptible to round-off error so we need to slightly relax the tolerance)
        assertTrue(nearlyEqual(skewness.get(fieldAKey), stats.getSkewness(fieldAKey), 1e-4));
        assertTrue(nearlyEqual(skewness.get(fieldBKey), stats.getSkewness(fieldBKey), 1e-4));
        // kurtosis (multi-pass is more susceptible to round-off error so we need to slightly relax the tolerance)
        assertTrue(nearlyEqual(kurtosis.get(fieldAKey), stats.getKurtosis(fieldAKey), 1e-4));
        assertTrue(nearlyEqual(kurtosis.get(fieldBKey), stats.getKurtosis(fieldBKey), 1e-4));
        // covariances
        assertTrue(nearlyEqual(covariances.get(fieldAKey).get(fieldBKey), stats.getCovariance(fieldAKey, fieldBKey), 1e-7));
        assertTrue(nearlyEqual(covariances.get(fieldBKey).get(fieldAKey), stats.getCovariance(fieldBKey, fieldAKey), 1e-7));
        // correlation
        assertTrue(nearlyEqual(correlations.get(fieldAKey).get(fieldBKey), stats.getCorrelation(fieldAKey, fieldBKey), 1e-7));
        assertTrue(nearlyEqual(correlations.get(fieldBKey).get(fieldAKey), stats.getCorrelation(fieldBKey, fieldAKey), 1e-7));
    }

    private static boolean nearlyEqual(double a, double b, double epsilon) {
        final double absA = Math.abs(a);
        final double absB = Math.abs(b);
        final double diff = Math.abs(a - b);

        if (a == b) { // shortcut, handles infinities
            return true;
        } else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
            // a or b is zero or both are extremely close to it
            // relative error is less meaningful here
            return diff < (epsilon * Double.MIN_NORMAL);
        } else { // use relative error
            return diff / Math.min((absA + absB), Double.MAX_VALUE) < epsilon;
        }
    }
}
