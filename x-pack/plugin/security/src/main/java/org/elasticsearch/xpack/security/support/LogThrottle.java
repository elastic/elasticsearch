/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.LongSupplier;

/**
 * Limits how often an action (such as emitting a log line) may run. Each instance tracks its own window.
 */
public final class LogThrottle {

    private final LongSupplier relativeTimeInMillis;
    private final long periodMs;

    private volatile long lastAcquireMs = -1;

    /**
     * Constructs a new instance using {@link ThreadPool#relativeTimeInMillis()} as the time source
     * and {@link TimeValue#millis()} as the period.
     */
    public LogThrottle(ThreadPool threadPool, TimeValue period) {
        this(threadPool::relativeTimeInMillis, period.millis());
    }

    private LogThrottle(LongSupplier relativeTimeInMillis, long periodMs) {
        this.relativeTimeInMillis = relativeTimeInMillis;
        this.periodMs = periodMs;
    }

    /**
     * @return {@code true} if the configured period has elapsed since the last {@code true} return
     *         (or if this is the first time the method has been called);
     *         updates the internal timestamp when returning {@code true}.
     *         <p>
     *         This method is <strong>not</strong> thread safe; concurrent {@code acquire} calls may
     *         both observe the window as elapsed, or have other races. Use a single-caller
     *         pattern or external synchronization if a hard guarantee is required.
     */
    public boolean acquire() {
        final long now = relativeTimeInMillis.getAsLong();
        if (lastAcquireMs < 0 || now - lastAcquireMs > periodMs) {
            lastAcquireMs = now;
            return true;
        }
        return false;
    }
}
