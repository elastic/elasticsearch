/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Lets a log site emit at most one line per interval at its loud level, however many objects share it. Held in a
 * {@code static} field, it bounds what a user who can query a failing dataset writes to the node log at {@code WARN}:
 * storage objects are created per file, split and resolution, so a per-object flag does not. Callers log the lines it
 * refuses at {@code DEBUG}.
 */
public final class LogThrottle {

    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private final AtomicLong nextAllowedNanos;

    public LogThrottle(TimeValue interval) {
        this(interval, System::nanoTime);
    }

    LogThrottle(TimeValue interval, LongSupplier nanoClock) {
        this.intervalNanos = interval.nanos();
        this.nanoClock = nanoClock;
        this.nextAllowedNanos = new AtomicLong(nanoClock.getAsLong());
    }

    /** Whether this call may log at the loud level; when it returns {@code true}, calls in the next interval get {@code false}. */
    public boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        long next = nextAllowedNanos.get();
        return now - next >= 0 && nextAllowedNanos.compareAndSet(next, now + intervalNanos);
    }

    /** Lets the next call log at the loud level; for tests that assert on a throttled site. */
    public void reset() {
        nextAllowedNanos.set(nanoClock.getAsLong());
    }
}
