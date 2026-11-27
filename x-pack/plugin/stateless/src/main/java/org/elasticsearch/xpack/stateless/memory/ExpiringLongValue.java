/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.core.Nullable;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper for a long that is only updated if the new value is bigger, up to {@code validUntil}.
 * An expired value is replaced by the next update even when that update is smaller.
 * Equal values keep the later {@code validUntil}, so publishing the same value again extends its lifetime.
 */
final class ExpiringLongValue {

    private final AtomicReference<Sample> ref = new AtomicReference<>();

    /**
     * Record {@code value} as valid from {@code nowInNanos} for {@code validityNanos}.
     * {@code value} must be non-negative
     * Keeps the current sample when it is still valid and larger, or when it is equal and expires later.
     */
    void update(long value, long nowInNanos, long validityNanos) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative but was [" + value + "]");
        }
        ref.accumulateAndGet(new Sample(value, nowInNanos + validityNanos), (existing, proposed) -> {
            if (existing == null || existing.validUntil() < nowInNanos) {
                return proposed;
            }
            return existing.compareTo(proposed) >= 0 ? existing : proposed;
        });
    }

    /**
     * The stored value when it is still valid at {@code nowInNanos}, otherwise {@code fallback}. An expired sample is cleared.
     */
    long valueIfValid(long nowInNanos, long fallback) {
        var sample = ref.get();
        if (sample == null || sample.validUntil() < nowInNanos) {
            // Try to clear the memory once the results are stale
            ref.compareAndSet(sample, null);
            return fallback;
        }
        return sample.value();
    }

    /**
     * The stored sample when it is still valid at {@code nowInNanos}, otherwise {@code null}. Does not clear an expired sample.
     */
    @Nullable
    Sample sampleIfValid(long nowInNanos) {
        var sample = ref.get();
        if (sample == null || sample.validUntil() < nowInNanos) {
            return null;
        }
        return sample;
    }

    void clear() {
        ref.set(null);
    }

    record Sample(long value, long validUntil) implements Comparable<Sample> {
        private static final Comparator<Sample> COMPARATOR = Comparator.comparingLong(Sample::value).thenComparingLong(Sample::validUntil);

        @Override
        public int compareTo(Sample other) {
            return COMPARATOR.compare(this, other);
        }
    }
}
