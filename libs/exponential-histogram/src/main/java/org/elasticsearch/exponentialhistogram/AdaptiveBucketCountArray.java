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

/**
 * A fixed-length array of non-negative bucket counts which stores its elements in the narrowest integer width that still fits every
 * value it has been given.
 * <p>
 * Bucket count arrays are allocated eagerly at the full bucket capacity of a histogram, but the counts themselves are almost always
 * tiny: a histogram of latencies over one collection interval rarely has a single bucket observed more than a hundred times, and the
 * great majority of its buckets are empty. Sizing every slot for the worst case (a {@code long}) therefore costs eight bytes per bucket
 * to represent numbers that overwhelmingly fit in one. That waste is irrelevant for a single histogram but dominates when thousands of
 * live histogram series are held in memory at once and charged against a circuit breaker.
 * <p>
 * So this array starts out as a {@code byte[]} and promotes itself in place — to {@code short[]}, then {@code int[]}, then
 * {@code long[]} — the first time a value is stored that the current width cannot hold. Promotion is one-way: a histogram that has
 * genuinely seen large counts keeps paying for them until it is {@link #demoteToMinimalWidth() reset}. Reads always return a
 * {@code long}, so callers cannot observe which width is currently in use. This mirrors {@code AdaptingIntegerArray} in the
 * OpenTelemetry Java SDK.
 * <p>
 * Instances are not thread-safe for writes, matching {@link FixedCapacityExponentialHistogram}.
 */
final class AdaptiveBucketCountArray {

    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(AdaptiveBucketCountArray.class);

    /**
     * The circuit breaker to charge width promotions to. Owned by whoever created this array; this class only ever adjusts the breaker
     * by the delta a promotion or demotion causes, never by the full size, so that the owner can keep accounting for the array as part
     * of its own {@code ramBytesUsed()}.
     */
    private final ExponentialHistogramCircuitBreaker circuitBreaker;

    private final int length;

    /**
     * The backing store, always exactly one of {@code byte[]}, {@code short[]}, {@code int[]} or {@code long[]}, all of {@link #length}
     * elements. A single reference rather than one field per width keeps the object header small, which matters because there is one of
     * these per histogram.
     */
    private Object counts;

    /**
     * @return the number of bytes a freshly created array of the given length occupies, i.e. before any promotion has happened
     */
    static long estimateSize(int length) {
        return SHALLOW_SIZE + RamEstimationUtil.estimateByteArray(length);
    }

    AdaptiveBucketCountArray(int length, ExponentialHistogramCircuitBreaker circuitBreaker) {
        this.length = length;
        this.circuitBreaker = circuitBreaker;
        this.counts = new byte[length];
    }

    private AdaptiveBucketCountArray(long[] values) {
        this.length = values.length;
        this.circuitBreaker = ExponentialHistogramCircuitBreaker.noop();
        this.counts = values;
    }

    /**
     * Presents an existing {@code long[]} as an adaptive array without copying or narrowing it. Used for the throw-away arrays built by
     * bucket iterators, which are neither long-lived nor accounted for, so there is nothing to gain from narrowing them.
     */
    static AdaptiveBucketCountArray wrapping(long[] values) {
        return new AdaptiveBucketCountArray(values);
    }

    int length() {
        return length;
    }

    long get(int slot) {
        Object store = counts;
        if (store instanceof byte[] bytes) {
            return bytes[slot];
        } else if (store instanceof short[] shorts) {
            return shorts[slot];
        } else if (store instanceof int[] ints) {
            return ints[slot];
        } else {
            return ((long[]) store)[slot];
        }
    }

    /**
     * Stores the given count, widening the backing store first if it does not fit.
     * <p>
     * If widening is required and the circuit breaker rejects it, this throws and leaves both the stored counts and the breaker exactly
     * as they were before the call.
     *
     * @param slot  the slot to write
     * @param value the count to store, must not be negative
     */
    void set(int slot, long value) {
        assert value >= 0 : "bucket counts must not be negative";
        Object store = counts;
        if (store instanceof byte[] bytes) {
            if (value <= Byte.MAX_VALUE) {
                bytes[slot] = (byte) value;
                return;
            }
        } else if (store instanceof short[] shorts) {
            if (value <= Short.MAX_VALUE) {
                shorts[slot] = (short) value;
                return;
            }
        } else if (store instanceof int[] ints) {
            if (value <= Integer.MAX_VALUE) {
                ints[slot] = (int) value;
                return;
            }
        } else {
            ((long[]) store)[slot] = value;
            return;
        }
        widenFor(value);
        set(slot, value);
    }

    /**
     * Returns this array to the narrowest width, giving the memory that any earlier promotion took back to the circuit breaker.
     * Only safe to call when the contents are about to be discarded, because it does not preserve them.
     */
    void demoteToMinimalWidth() {
        if (counts instanceof byte[]) {
            return;
        }
        long delta = RamEstimationUtil.estimateByteArray(length) - storeSize(counts);
        counts = new byte[length];
        // never positive, so this cannot throw
        circuitBreaker.adjustBreaker(delta);
    }

    long ramBytesUsed() {
        return SHALLOW_SIZE + storeSize(counts);
    }

    /**
     * Replaces the backing store with the narrowest one that can hold the given value, preserving the current contents.
     */
    private void widenFor(long value) {
        long currentSize = storeSize(counts);
        long widenedSize;
        if (value <= Short.MAX_VALUE) {
            widenedSize = RamEstimationUtil.estimateShortArray(length);
        } else if (value <= Integer.MAX_VALUE) {
            widenedSize = RamEstimationUtil.estimateIntArray(length);
        } else {
            widenedSize = RamEstimationUtil.estimateLongArray(length);
        }
        long delta = widenedSize - currentSize;
        // charge before allocating, so that a rejected promotion leaves this array untouched
        circuitBreaker.adjustBreaker(delta);
        try {
            if (value <= Short.MAX_VALUE) {
                short[] widened = new short[length];
                byte[] current = (byte[]) counts;
                for (int i = 0; i < length; i++) {
                    widened[i] = current[i];
                }
                counts = widened;
            } else if (value <= Integer.MAX_VALUE) {
                int[] widened = new int[length];
                for (int i = 0; i < length; i++) {
                    widened[i] = (int) get(i);
                }
                counts = widened;
            } else {
                long[] widened = new long[length];
                for (int i = 0; i < length; i++) {
                    widened[i] = get(i);
                }
                counts = widened;
            }
        } catch (RuntimeException | Error e) {
            circuitBreaker.adjustBreaker(-delta);
            throw e;
        }
    }

    private static long storeSize(Object store) {
        if (store instanceof byte[] bytes) {
            return RamEstimationUtil.estimateByteArray(bytes.length);
        } else if (store instanceof short[] shorts) {
            return RamEstimationUtil.estimateShortArray(shorts.length);
        } else if (store instanceof int[] ints) {
            return RamEstimationUtil.estimateIntArray(ints.length);
        } else {
            return RamEstimationUtil.estimateLongArray(((long[]) store).length);
        }
    }
}
