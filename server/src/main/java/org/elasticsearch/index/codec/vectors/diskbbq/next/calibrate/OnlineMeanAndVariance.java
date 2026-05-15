/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next.calibrate;

/**
 * Welford's online algorithm for computing mean and variance in a single pass.
 * Uses Bessel's correction (divides by n-1) for sample variance.
 */
final class OnlineMeanAndVariance {

    private long n;
    private double mean;
    private double m2;

    OnlineMeanAndVariance() {}

    void add(double x) {
        n++;
        double delta = x - mean;
        mean += delta / n;
        m2 += delta * (x - mean);
    }

    double mean() {
        return mean;
    }

    double var() {
        return n <= 1 ? 0.0 : m2 / (n - 1);
    }
}
