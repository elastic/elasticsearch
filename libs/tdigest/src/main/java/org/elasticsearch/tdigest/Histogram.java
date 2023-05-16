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

import java.util.Locale;

/**
 * A Histogram is a histogram with cleverly chosen, but fixed, bin widths.
 *
 * Different implementations may provide better or worse speed or space complexity,
 * but each is attuned to a particular distribution or error metric.
 */
@SuppressWarnings("WeakerAccess")
public abstract class Histogram {
    protected long[] counts;
    protected double min;
    protected double max;
    protected double logFactor;
    protected double logOffset;

    public Histogram(double min, double max) {
        this.min = min;
        this.max = max;
    }

    protected void setupBins(double min, double max) {
        int binCount = bucketIndex(max) + 1;
        if (binCount > 10000) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Excessive number of bins %d resulting from min,max = %.2g, %.2g", binCount, min, max)
            );

        }
        counts = new long[binCount];
    }

    public void add(double v) {
        counts[bucket(v)]++;
    }

    @SuppressWarnings("WeakerAccess")
    public double[] getBounds() {
        double[] r = new double[counts.length];
        for (int i = 0; i < r.length; i++) {
            r[i] = lowerBound(i);
        }
        return r;
    }

    public long[] getCounts() {
        return counts;
    }

    // exposed for testing
    int bucket(double x) {
        if (x <= min) {
            return 0;
        } else if (x >= max) {
            return counts.length - 1;
        } else {
            return bucketIndex(x);
        }
    }

    protected abstract int bucketIndex(double x);

    // exposed for testing
    abstract double lowerBound(int k);

    @SuppressWarnings("WeakerAccess")
    abstract long[] getCompressedCounts();

    abstract void add(Iterable<Histogram> others);
}
