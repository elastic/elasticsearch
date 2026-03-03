/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.elasticsearch.test.ESTestCase;

import java.util.*;

public class OnlineQuantileEstimatorTests extends ESTestCase {

    /** Tests the convergence to the true value of the q-quantile estimator using an uniform distribution for q=0.25, 0.5, 0.75. */
    public void testQuantileUniform() {
        final int nTrials = 100;
        final int n = 100_000;

        for (float q : List.of(0.25f, 0.5f, 0.75f)) {
            OnlineQuantileEstimator fq = new OnlineQuantileEstimator(q, 0f, 1f, 0.0001f, 42L);
            for (int trial = 0; trial < nTrials; trial++) {
                for (int i = 0; i < n; i++) {
                    float value = random().nextFloat();
                    fq.updateEstimate(value);
                }
                double quantileEstimate = fq.getEstimate();
                fq.reset();
                assertTrue(Math.abs(quantileEstimate - q) < 0.03f);
            }
        }
    }

    /** Tests the convergence to the true value of the q-quantile estimator using an exponential distribution for q=0.25, 0.5, 0.75. */
    public void testQuantileExponential() {
        final int nTrials = 100;
        final int n = 100_000;
        final float lambda = 1;
        float min = 0f;
        float max = (float) -Math.log(1e-6) / lambda;

        for (float q : List.of(0.25f, 0.5f, 0.75f)) {
            OnlineQuantileEstimator fq = new OnlineQuantileEstimator(q, min, max, 0.00001f, 42L);
            for (int trial = 0; trial < nTrials; trial++) {
                for (int i = 0; i < n; i++) {
                    float value = (float) -Math.log(1f - random().nextFloat()) / lambda;
                    fq.updateEstimate(value);
                }
                double quantileEstimate = fq.getEstimate();
                fq.reset();
                // Higher quantiles of an exponential distribution have more error, and the threshold tries to account for it for small
                // (the current threshold is rather conservative for small q).
                // The quantiles of the exponential distribution are given by -Math.log(1 - q) / lambda
                assertTrue(Math.abs(quantileEstimate + Math.log(1 - q) / lambda) < 0.02f * Math.pow(2.4, 1 + q));
            }
        }
    }
}
