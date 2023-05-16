/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    abstract void add(Iterable<Histogram> others);
}
