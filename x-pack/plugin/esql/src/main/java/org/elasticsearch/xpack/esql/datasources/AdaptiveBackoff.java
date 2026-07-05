/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Shared adaptive backoff state for a storage provider. When throttling is detected,
 * the backoff multiplier increases (up to a cap); on sustained success, it decays back
 * to 1x. Thread-safe via atomic operations.
 * <p>
 * The multiplier is applied by {@link RetryPolicy#delayMillis} to scale retry delays
 * for throttling errors, creating a global slowdown effect that prevents thundering herd
 * recovery.
 * <p>
 * Decay is time-based: the multiplier halves for every {@link #DECAY_INTERVAL_NANOS}
 * elapsed since the last throttle event, but only when {@link #onSuccess()} is called
 * (i.e., decay requires active successful operations, not just passage of time).
 */
class AdaptiveBackoff {

    private static final Logger logger = LogManager.getLogger(AdaptiveBackoff.class);

    static final AdaptiveBackoff DISABLED = new AdaptiveBackoff(0, System::nanoTime);

    static final int MAX_MULTIPLIER = 16;
    static final long DECAY_INTERVAL_NANOS = 10_000_000_000L; // 10 seconds

    private final int maxMultiplier;
    private final LongSupplier nanoTimeSupplier;
    private final AtomicInteger multiplier = new AtomicInteger(1);
    private final AtomicLong lastThrottleNanos = new AtomicLong(0);

    AdaptiveBackoff(int maxMultiplier, LongSupplier nanoTimeSupplier) {
        this.maxMultiplier = maxMultiplier;
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    AdaptiveBackoff() {
        this(MAX_MULTIPLIER, System::nanoTime);
    }

    void onThrottled() {
        if (maxMultiplier <= 0) {
            return;
        }
        lastThrottleNanos.set(nanoTimeSupplier.getAsLong());
        int oldVal = multiplier.getAndUpdate(m -> Math.min(m * 2, maxMultiplier));
        int newVal = multiplier.get();
        if (oldVal != newVal) {
            logger.debug("adaptive backoff multiplier changed from [{}]x to [{}]x after throttling", oldVal, newVal);
        }
    }

    void onSuccess() {
        if (maxMultiplier <= 0) {
            return;
        }
        int current = multiplier.get();
        if (current <= 1) {
            return;
        }
        long now = nanoTimeSupplier.getAsLong();
        long lastThrottle = lastThrottleNanos.get();
        long elapsed = (now - lastThrottle) / DECAY_INTERVAL_NANOS;
        if (elapsed < 1) {
            return;
        }
        // Only apply decay if we successfully advance the baseline — prevents concurrent
        // threads from double-counting the same elapsed intervals
        if (lastThrottleNanos.compareAndSet(lastThrottle, lastThrottle + elapsed * DECAY_INTERVAL_NANOS)) {
            int oldVal = multiplier.getAndUpdate(m -> {
                int decayed = m;
                for (long i = 0; i < elapsed && decayed > 1; i++) {
                    decayed = Math.max(1, decayed / 2);
                }
                return decayed;
            });
            int newVal = multiplier.get();
            if (oldVal != newVal) {
                logger.debug("adaptive backoff multiplier decayed from [{}]x to [{}]x after success", oldVal, newVal);
            }
        }
    }

    int currentMultiplier() {
        if (maxMultiplier <= 0) {
            return 1;
        }
        return multiplier.get();
    }

    boolean isEnabled() {
        return maxMultiplier > 0;
    }
}
