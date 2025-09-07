/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
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
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.RamUsageEstimator;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareExponentiallyScaledValues;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.exponentiallyScaledToDoubleValue;

/**
 * Represents the bucket for values around zero in an exponential histogram.
 * Range: [-zeroThreshold, +zeroThreshold].
 *
 * Refactor (Task 1):
 * - Added static factory methods (fromThreshold, fromIndexAndScale,
 * minimalEmpty).
 * - Replaced sentinel lazy state (Long.MAX_VALUE / Double.NaN) with explicit
 * booleans
 * (indexComputed, thresholdComputed).
 * - Added equals, hashCode, toString for value semantics.
 * - Added package-private isIndexComputed()/isThresholdComputed() for tests.
 */
public final class ZeroBucket {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ZeroBucket.class);

    private final int scale;
    private long index;
    private double realThreshold;
    private final long count;

    private boolean indexComputed;
    private boolean thresholdComputed;

    // Singleton minimal empty
    private static final ZeroBucket MINIMAL_EMPTY = new ZeroBucket(
            0L,
            MIN_SCALE,
            0L,
            true,
            true,
            0.0d);

    /* ===================== Factory Methods ===================== */

    public static ZeroBucket fromThreshold(double zeroThreshold, long count) {
        if (zeroThreshold < 0.0) {
            throw new IllegalArgumentException("zeroThreshold must be >= 0 (was " + zeroThreshold + ")");
        }
        return new ZeroBucket(
                0L, // placeholder until index computed
                MAX_SCALE,
                count,
                false, // index not computed yet
                true, // threshold known
                zeroThreshold);
    }

    public static ZeroBucket fromIndexAndScale(long index, int scale, long count) {
        if (scale < MIN_SCALE || scale > MAX_SCALE) {
            throw new IllegalArgumentException("scale out of range: " + scale);
        }
        return new ZeroBucket(
                index,
                scale,
                count,
                true, // index known
                false, // threshold lazy
                Double.NaN);
    }

    public static ZeroBucket minimalEmpty() {
        return MINIMAL_EMPTY;
    }

    /* ===================== Private Constructor ===================== */
    private ZeroBucket(
            long index,
            int scale,
            long count,
            boolean indexComputed,
            boolean thresholdComputed,
            double realThreshold) {
        this.index = index;
        this.scale = scale;
        this.count = count;
        this.indexComputed = indexComputed;
        this.thresholdComputed = thresholdComputed;
        this.realThreshold = realThreshold;
    }

    /* ===================== Public API ===================== */

    public long count() {
        return count;
    }

    public long index() {
        computeIndexIfNeeded();
        return index;
    }

    public double zeroThreshold() {
        computeThresholdIfNeeded();
        return realThreshold;
    }

    public int scale() {
        return scale;
    }

    /* ===================== Lazy Computation ===================== */

    private void computeIndexIfNeeded() {
        if (indexComputed == false) {
            index = computeIndex(realThreshold, scale);
            indexComputed = true;
        }
    }

    private void computeThresholdIfNeeded() {
        if (thresholdComputed == false) {
            realThreshold = exponentiallyScaledToDoubleValue(index, scale);
            thresholdComputed = true;
        }
    }

    /* ===================== Package-Private for Tests ===================== */

    boolean isIndexComputed() {
        return indexComputed;
    }

    boolean isThresholdComputed() {
        return thresholdComputed;
    }

    /* ===================== Comparison Helper ===================== */

    int compareTo(long otherIndex, int otherScale) {
        return compareExponentiallyScaledValues(index(), scale, otherIndex, otherScale);
    }

    /* ===================== Value Semantics ===================== */

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof ZeroBucket zb) {
            long i1 = index();
            long i2 = zb.index();
            double t1 = zeroThreshold();
            double t2 = zb.zeroThreshold();
            return scale == zb.scale && count == zb.count && i1 == i2 && Double.compare(t1, t2) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = Integer.hashCode(scale);
        h = 31 * h + Long.hashCode(index());
        h = 31 * h + Double.hashCode(zeroThreshold());
        h = 31 * h + Long.hashCode(count);
        return h;
    }

    @Override
    public String toString() {
        return "ZeroBucket{scale=" + scale
                + ", index=" + index()
                + ", threshold=" + zeroThreshold()
                + ", count=" + count
                + ", indexComputed=" + indexComputed
                + ", thresholdComputed=" + thresholdComputed
                + "}";
    }
}