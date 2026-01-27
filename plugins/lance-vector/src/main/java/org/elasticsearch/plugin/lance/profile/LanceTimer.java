/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.profile;

/**
 * A timer that automatically records timing when used with try-with-resources.
 * <p>
 * This class measures execution time in milliseconds and records it to the
 * thread-local {@link LanceTimingContext} when the timer is closed.
 * <p>
 * Usage example:
 * <pre>{@code
 * try (var timer = new LanceTimer(LanceTimingStage.NATIVE_SEARCH_EXECUTION)) {
 *     // code to time
 * } // timing is automatically recorded when the block exits
 * }</pre>
 * <p>
 * Timing is only recorded when profiling is active. When profiling is disabled,
 * the timer has minimal overhead (a single null check).
 */
public class LanceTimer implements AutoCloseable {

    private final LanceTimingContext.LanceTimingStage stage;
    private final long startTimeNanos;
    private final LanceTimingContext context;

    /**
     * Create a new timer for the specified timing stage.
     * <p>
     * The timer starts immediately upon construction and stops when {@link #close()}
     * is called (typically automatically via try-with-resources).
     *
     * @param stage The timing stage to record
     */
    public LanceTimer(LanceTimingContext.LanceTimingStage stage) {
        this.stage = stage;
        this.startTimeNanos = System.nanoTime();
        this.context = LanceTimingContext.getOrNull();
    }

    /**
     * Stop the timer and record the elapsed time to the timing context.
     * <p>
     * This method is idempotent - calling it multiple times has no additional effect.
     * Timing is only recorded if profiling is active.
     */
    @Override
    public void close() {
        if (context != null && context.isActive()) {
            long elapsedNanos = System.nanoTime() - startTimeNanos;
            long elapsedMs = nanosToMillis(elapsedNanos);
            context.record(stage, elapsedMs);
        }
    }

    /**
     * Convert nanoseconds to milliseconds.
     * <p>
     * This method rounds to the nearest millisecond to provide
     * human-readable timing values without losing precision for short operations.
     *
     * @param nanos Duration in nanoseconds
     * @return Duration in milliseconds (rounded)
     */
    private static long nanosToMillis(long nanos) {
        return (nanos + 500_000L) / 1_000_000L;
    }

    /**
     * Get the current elapsed time in milliseconds without stopping the timer.
     * <p>
     * This can be useful for logging or debugging purposes.
     *
     * @return Elapsed time in milliseconds since construction
     */
    public long elapsedMillis() {
        long elapsedNanos = System.nanoTime() - startTimeNanos;
        return nanosToMillis(elapsedNanos);
    }
}
