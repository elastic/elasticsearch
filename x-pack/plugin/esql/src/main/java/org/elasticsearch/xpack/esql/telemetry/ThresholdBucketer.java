/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.concurrent.atomic.LongAdder;

/**
 * Generic threshold-based histogram bucketer for phone-home (XPack usage) telemetry.
 * Counts observations into N fixed buckets defined by N-1 ascending thresholds, then
 * serialises all bucket counts into a {@link Counters} instance.
 * <p>
 * {@link TookMetrics} is the primary user — it is a thin wrapper over this class with a
 * hardcoded time ladder. Datasource distributions that need count or byte ladders create
 * their own instances with the appropriate thresholds.
 */
public final class ThresholdBucketer {

    private final long[] thresholds;
    private final String[] suffixes;
    private final LongAdder[] buckets;

    /**
     * @param thresholds ascending threshold values; a value {@code v} falls into bucket {@code i}
     *                   when {@code v < thresholds[i]}, or into the last bucket when {@code v >= thresholds[thresholds.length-1]}.
     * @param suffixes   bucket label suffixes, one more entry than {@code thresholds}; appended to the
     *                   prefix in {@link #counters(String, Counters)}.
     */
    public ThresholdBucketer(long[] thresholds, String[] suffixes) {
        if (thresholds.length != suffixes.length - 1) {
            throw new IllegalArgumentException(
                "thresholds.length [" + thresholds.length + "] must equal suffixes.length - 1 [" + (suffixes.length - 1) + "]"
            );
        }
        this.thresholds = thresholds.clone();
        this.suffixes = suffixes.clone();
        this.buckets = new LongAdder[suffixes.length];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /** Increments the bucket that contains {@code value}. */
    public void count(long value) {
        for (int i = 0; i < thresholds.length; i++) {
            if (value < thresholds[i]) {
                buckets[i].increment();
                return;
            }
        }
        buckets[thresholds.length].increment();
    }

    /** Serialises all bucket counts into {@code counters} using keys of the form {@code prefix + suffix}. */
    public void counters(String prefix, Counters counters) {
        for (int i = 0; i < buckets.length; i++) {
            counters.inc(prefix + suffixes[i], buckets[i].sum());
        }
    }
}
