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

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareExponentiallyScaledValues;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.exponentiallyScaledToDoubleValue;

/**
 * Represents the bucket for values around zero in an exponential histogram.
 * The range of this bucket is {@code [-zeroThreshold, +zeroThreshold]}.
 *
 * Refactor (Task 1):
 * - Added static factories (fromThreshold, fromIndexAndScale) while keeping
 * original public constructor
 * - Introduced explicit lazy flags (indexComputed, thresholdComputed) instead
 * of sentinels
 * - Added value semantics (equals, hashCode, toString)
 * - Preserved original API: minimalEmpty, minimalWithCount, merge,
 * collapseOverlappingBuckets[ForAll], compareZeroThreshold
 */
public final class ZeroBucket {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ZeroBucket.class);

    private final int scale;
    private long index;
    private double realThreshold;
    private final long count;

    // Explicit lazy flags
    private boolean indexComputed;
    private boolean thresholdComputed;

    // Original minimal empty singleton (index known, threshold lazy)
    private static final ZeroBucket MINIMAL_EMPTY = new ZeroBucket(
            MIN_INDEX,
            MIN_SCALE,
            0L,
            true,
            false,
            Double.NaN);

    /*
     * ===================== Original Public Constructor (kept)
     * =====================
     */

    /**
     * Original public constructor (threshold authoritative). Kept for source
     * compatibility.
     * Deprecated in favor of {@link #fromThreshold(double, long)}.
     */
    @Deprecated
    public ZeroBucket(double zeroThreshold, long count) {
        if (zeroThreshold < 0.0) {
            throw new IllegalArgumentException("zeroThreshold must be >= 0 (was " + zeroThreshold + ")");
        }
        this.scale = MAX_SCALE;
        this.count = count;
        this.realThreshold = zeroThreshold;
        this.index = 0L; // placeholder until computed
        this.indexComputed = false;
        this.thresholdComputed = true;
    }

    /* ===================== Factory Methods ===================== */

    /**
     * Create a ZeroBucket from an explicit threshold (index lazy).
     */
    public static ZeroBucket fromThreshold(double zeroThreshold, long count) {
        if (zeroThreshold < 0.0) {
            throw new IllegalArgumentException("zeroThreshold must be >= 0 (was " + zeroThreshold + ")");
        }
        return new ZeroBucket(
                0L,
                MAX_SCALE,
                count,
                false,
                true,
                zeroThreshold);
    }

    /**
     * Create a ZeroBucket from an index + scale (threshold lazy).
     */
    public static ZeroBucket fromIndexAndScale(long index, int scale, long count) {
        if (scale < MIN_SCALE || scale > MAX_SCALE) {
            throw new IllegalArgumentException("scale out of range: " + scale);
        }
        if (index < MIN_INDEX || index > MAX_INDEX) {
            throw new IllegalArgumentException("index out of range: " + index);
        }
        return new ZeroBucket(
                index,
                scale,
                count,
                true,
                false,
                Double.NaN);
    }

    /**
     * @return singleton empty minimal bucket.
     */
    public static ZeroBucket minimalEmpty() {
        return MINIMAL_EMPTY;
    }

    /**
     * Creates a zero bucket with the smallest possible threshold and a given count.
     * If count == 0 returns the singleton.
     */
    public static ZeroBucket minimalWithCount(long count) {
        if (count == 0) {
            return MINIMAL_EMPTY;
        }
        // Resolve lazy threshold & index of singleton
        double threshold = MINIMAL_EMPTY.zeroThreshold();
        long idx = MINIMAL_EMPTY.index();
        return resolved(threshold, idx, MINIMAL_EMPTY.scale(), count);
    }

    /* ===================== Private Constructors ===================== */

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

    private static ZeroBucket resolved(double threshold, long index, int scale, long count) {
        return new ZeroBucket(index, scale, count, true, true, threshold);
    }

    /* ===================== Accessors ===================== */

    public long count() {
        return count;
    }

    public int scale() {
        return scale;
    }

    /**
     * Returns index; if threshold authoritative, compute with +1 rule (matches
     * original code).
     */
    public long index() {
        computeIndexIfNeeded();
        return index;
    }

    /**
     * Returns threshold; if index authoritative compute it lazily.
     */
    public double zeroThreshold() {
        computeThresholdIfNeeded();
        return realThreshold;
    }

    /* ===================== Lazy Computation Helpers ===================== */

    private void computeIndexIfNeeded() {
        if (indexComputed == false) {
            index = computeIndex(realThreshold, scale) + 1;
            indexComputed = true;
        }
    }

    private void computeThresholdIfNeeded() {
        if (thresholdComputed == false) {
            realThreshold = exponentiallyScaledToDoubleValue(index(), scale);
            thresholdComputed = true;
        }
    }

    /* Package-private for tests */
    boolean isIndexComputed() {
        return indexComputed;
    }

    boolean isThresholdComputed() {
        return thresholdComputed;
    }

    /* ===================== Original Functional API ===================== */

    public int compareZeroThreshold(ZeroBucket other) {
        return compareExponentiallyScaledValues(index(), scale(), other.index(), other.scale());
    }

    public ZeroBucket merge(ZeroBucket other) {
        if (other.count == 0) {
            return this;
        } else if (this.count == 0) {
            return other;
        } else {
            long total = this.count + other.count;
            if (this.compareZeroThreshold(other) >= 0) {
                return resolved(this.zeroThreshold(), this.index(), this.scale(), total);
            } else {
                return resolved(other.zeroThreshold(), other.index(), other.scale(), total);
            }
        }
    }

    public ZeroBucket collapseOverlappingBucketsForAll(BucketIterator... bucketIterators) {
        ZeroBucket current = this;
        ZeroBucket previous;
        do {
            previous = current;
            for (BucketIterator b : bucketIterators) {
                current = current.collapseOverlappingBuckets(b);
            }
        } while (previous.compareZeroThreshold(current) != 0);
        return current;
    }

    public ZeroBucket collapseOverlappingBuckets(BucketIterator buckets) {
        long collapsedCount = 0;
        long highestCollapsedIndex = 0;
        while (buckets.hasNext()
                && compareExponentiallyScaledValues(buckets.peekIndex(), buckets.scale(), index(), scale()) < 0) {
            highestCollapsedIndex = buckets.peekIndex();
            collapsedCount += buckets.peekCount();
            buckets.advance();
        }
        if (collapsedCount == 0) {
            return this;
        } else {
            long newZeroCount = count + collapsedCount;
            long collapsedUpperBoundIndex = highestCollapsedIndex + 1;
            if (compareExponentiallyScaledValues(index(), scale(), collapsedUpperBoundIndex, buckets.scale()) >= 0) {
                return resolved(this.zeroThreshold(), this.index(), this.scale(), newZeroCount);
            } else {
                return fromIndexAndScale(collapsedUpperBoundIndex, buckets.scale(), newZeroCount);
            }
        }
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