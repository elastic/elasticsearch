/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

/**
 * Utility for measuring per-thread CPU time in drivers and format readers.
 * <p>
 * Call {@link #currentNanos()} at the start of a timed region and pass the reading to
 * {@link #elapsedNanos(long)} at the end, or to {@link #elapsedNanos(long, long)} with an end reading.
 * A reading of {@code -1} means the JVM does not support per-thread CPU timing (e.g., GraalVM Native
 * Image). Both {@code elapsedNanos} overloads return {@code 0} for it, so only callers that subtract
 * raw readings themselves need to guard with {@code if (start >= 0)}.
 */
public final class ThreadCpuTimer {

    private static final ThreadMXBean THREAD_MX = ManagementFactory.getThreadMXBean();

    /**
     * {@link ThreadMXBean#getCurrentThreadCpuTime()} throws rather than returning {@code -1} when
     * the JVM doesn't support thread CPU time, so check once up front.
     */
    private static final boolean SUPPORTED = THREAD_MX.isCurrentThreadCpuTimeSupported();

    private ThreadCpuTimer() {}

    /**
     * Returns the CPU time consumed by the current thread in nanoseconds, or
     * {@code -1} if {@link ThreadMXBean#isCurrentThreadCpuTimeSupported()} is
     * {@code false} or CPU-time collection has been disabled at runtime.
     */
    public static long currentNanos() {
        return SUPPORTED ? THREAD_MX.getCurrentThreadCpuTime() : -1;
    }

    /**
     * Returns the CPU nanos elapsed since {@code startCpuNanos}, clamped to zero, or {@code 0} if
     * either reading is {@code -1}.
     */
    public static long elapsedNanos(long startCpuNanos) {
        return elapsedNanos(startCpuNanos, currentNanos());
    }

    /**
     * Returns the CPU nanos between two readings of the same thread's clock, clamped to zero, or
     * {@code 0} if either reading is {@code -1}. Takes the end reading so a caller can reuse it as
     * the start of its next interval.
     */
    public static long elapsedNanos(long startCpuNanos, long endCpuNanos) {
        if (startCpuNanos < 0 || endCpuNanos < 0) {
            return 0;
        }
        return Math.max(0L, endCpuNanos - startCpuNanos);
    }
}
