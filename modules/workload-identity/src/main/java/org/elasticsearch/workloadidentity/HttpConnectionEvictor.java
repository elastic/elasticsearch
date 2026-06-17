/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Background task that periodically reaps expired and idle connections from an Apache HC 4.x
 * {@link NHttpClientConnectionManager}. Apache HC 4.x has no built-in stale-connection eviction,
 * so the host process must schedule it explicitly; HC 5.x makes this class unnecessary.
 *
 * <p>A near-duplicate of this primitive already exists in {@code x-pack/plugin/inference}
 * ({@code IdleConnectionEvictor}); the two should be consolidated into a shared component.
 *
 * <p>Runs on the {@link ThreadPool#generic() generic} executor.
 */
public final class HttpConnectionEvictor implements Closeable {

    private static final Logger logger = LogManager.getLogger(HttpConnectionEvictor.class);

    private final ThreadPool threadPool;
    private final NHttpClientConnectionManager connectionManager;
    private final TimeValue interval;
    private final TimeValue maxIdleTime;
    private final AtomicReference<Scheduler.Cancellable> cancellable = new AtomicReference<>();

    public HttpConnectionEvictor(
        ThreadPool threadPool,
        NHttpClientConnectionManager connectionManager,
        TimeValue interval,
        TimeValue maxIdleTime
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.interval = Objects.requireNonNull(interval);
        this.maxIdleTime = Objects.requireNonNull(maxIdleTime);
    }

    /**
     * Start scheduling eviction passes. Idempotent; calling {@code start()} more than once
     * has no effect while the previous schedule is still active.
     */
    public synchronized void start() {
        if (cancellable.get() != null) {
            return;
        }
        logger.debug("starting HTTP connection evictor: interval=[{}], max_idle=[{}]", interval, maxIdleTime);
        cancellable.set(threadPool.scheduleWithFixedDelay(this::evict, interval, threadPool.generic()));
    }

    private void evict() {
        try {
            connectionManager.closeExpiredConnections();
            connectionManager.closeIdleConnections(maxIdleTime.millis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.warn("HTTP connection eviction failed", e);
        }
    }

    /** @return {@code true} when an eviction task is currently scheduled (and not yet cancelled). */
    public boolean isRunning() {
        final Scheduler.Cancellable task = cancellable.get();
        return task != null && task.isCancelled() == false;
    }

    @Override
    public void close() {
        final Scheduler.Cancellable task = cancellable.getAndSet(null);
        if (task != null) {
            logger.debug("closing HTTP connection evictor");
            task.cancel();
        }
    }
}
