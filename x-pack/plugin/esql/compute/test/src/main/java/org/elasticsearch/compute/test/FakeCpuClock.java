/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import java.util.function.LongSupplier;

/**
 * A per-thread CPU clock the test advances by hand, for CPU trackers. Each thread has its own counter, like
 * {@code ThreadMXBean#getCurrentThreadCpuTime()}, so CPU time only passes where the test burns it.
 */
public final class FakeCpuClock implements LongSupplier {
    private final ThreadLocal<long[]> cpu = ThreadLocal.withInitial(() -> new long[1]);

    /** Advances the calling thread's CPU time. */
    public void burn(long nanos) {
        cpu.get()[0] += nanos;
    }

    @Override
    public long getAsLong() {
        return cpu.get()[0];
    }
}
