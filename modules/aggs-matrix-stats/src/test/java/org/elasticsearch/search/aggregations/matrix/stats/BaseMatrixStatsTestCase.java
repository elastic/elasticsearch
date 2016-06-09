/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public abstract class BaseMatrixStatsTestCase extends ESTestCase {
    protected final int numObs = atLeast(10000);
    protected final ArrayList<Double> fieldA = new ArrayList<>(numObs);
    protected final ArrayList<Double> fieldB = new ArrayList<>(numObs);
    protected final MultiPassStats actualStats = new MultiPassStats();
    protected final static String fieldAKey = "fieldA";
    protected final static String fieldBKey = "fieldB";

    @Before
    public void setup() {
        createStats();
    }

    public void createStats() {
        for (int n = 0; n < numObs; ++n) {
            fieldA.add(randomDouble());
            fieldB.add(randomDouble());
        }
        actualStats.computeStats(fieldA, fieldB);
    }

    static class MultiPassStats {
        long count;
        HashMap<String, Double> means = new HashMap<>();
        HashMap<String, Double> variances = new HashMap<>();
        HashMap<String, Double> skewness = new HashMap<>();
        HashMap<String, Double> kurtosis = new HashMap<>();
        HashMap<String, HashMap<String, Double>> covariances = new HashMap<>();
        HashMap<String, HashMap<String, Double>> correlations = new HashMap<>();

        @SuppressWarnings("unchecked")
        void computeStats(final ArrayList<Double> fieldA, final ArrayList<Double> fieldB) {
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
            means.put(fieldAKey, meanA/count);
            means.put(fieldBKey, meanB/count);

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
            final HashMap<String, Double> fieldACovar = new HashMap<>(2);
            fieldACovar.put(fieldAKey, 1d);
            cVar /= count - 1;
            fieldACovar.put(fieldBKey, cVar);
            covariances.put(fieldAKey, fieldACovar);
            final HashMap<String, Double> fieldBCovar = new HashMap<>(2);
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

        public void assertNearlyEqual(MatrixStatsResults stats) {
            assertThat(count, equalTo(stats.getDocCount()));
            assertThat(count, equalTo(stats.getFieldCount(fieldAKey)));
            assertThat(count, equalTo(stats.getFieldCount(fieldBKey)));
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
