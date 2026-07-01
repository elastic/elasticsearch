/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.concurrent.Executor;

/**
 * Schedules a retry continuation to run after a backoff delay <b>without holding a thread during the wait</b>.
 * <p>
 * The async read-retry path ({@code RetryableStorageObject.readBytesAsyncWithRetry}) uses this instead of parking
 * a pool thread on {@code Thread.sleep} for the backoff: the continuation is handed to a timer and runs on the
 * supplied {@code executor} once the delay elapses, so no worker thread is occupied while waiting. Production wires
 * a {@code ThreadPool}-backed implementation (see {@code DataSourceModule}); the synchronous read paths keep their
 * own (caller-thread) backoff because a blocking {@code InputStream.read()} has no continuation to reschedule.
 */
@FunctionalInterface
interface RetryScheduler {

    /**
     * Runs {@code command} on {@code executor} after {@code delayMillis} (or promptly when {@code delayMillis <= 0}).
     * Must not block the calling thread for the duration of the delay.
     */
    void schedule(Runnable command, long delayMillis, Executor executor);

    /**
     * Fallback that runs the continuation on the executor immediately, ignoring the delay — used in tests and any
     * context with no {@code ThreadPool}. It never blocks, but it does not honor the backoff delay, so production
     * always wires a real scheduler instead.
     */
    RetryScheduler DIRECT = (command, delayMillis, executor) -> executor.execute(command);
}
