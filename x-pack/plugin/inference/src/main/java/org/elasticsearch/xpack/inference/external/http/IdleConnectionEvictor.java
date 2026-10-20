/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Starts a monitoring task to remove expired and idle connections from the HTTP connection pool.
 *
 * The http client library ships an equivalent evictor thread, but it is configured once at client build time. We keep this
 * implementation because the eviction interval and max idle time are dynamic cluster settings, and because it schedules on the
 * Elasticsearch thread pool instead of spawning a dedicated thread.
 *
 * TODO (httpclient5 migration): the migration plan proposed deleting this in favor of
 * HttpAsyncClientBuilder#evictIdleConnections/#evictExpiredConnections; that was intentionally not done to keep the
 * connection_eviction_* settings dynamic. If those settings ever become node-restart-scoped, switch to the built-in evictor.
 *
 * See <a href="https://hc.apache.org/httpcomponents-client-5.5.x/current/httpclient5/apidocs/org/apache/hc/client5/http/impl/IdleConnectionEvictor.html">here for more info.</a>
 */
public class IdleConnectionEvictor implements Closeable {
    private static final Logger logger = LogManager.getLogger(IdleConnectionEvictor.class);

    private final ThreadPool threadPool;
    private final PoolingAsyncClientConnectionManager connectionManager;
    private final TimeValue sleepTime;
    private final AtomicReference<TimeValue> maxIdleTime = new AtomicReference<>();
    private final AtomicReference<Scheduler.Cancellable> cancellableTask = new AtomicReference<>();

    public IdleConnectionEvictor(
        ThreadPool threadPool,
        PoolingAsyncClientConnectionManager connectionManager,
        TimeValue sleepTime,
        @Nullable TimeValue maxIdleTime
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.sleepTime = Objects.requireNonNull(sleepTime);
        this.maxIdleTime.set(maxIdleTime);
    }

    public void setMaxIdleTime(TimeValue maxIdleTime) {
        this.maxIdleTime.set(maxIdleTime);
    }

    public synchronized void start() {
        if (cancellableTask.get() == null) {
            startInternal();
        }
    }

    private void startInternal() {
        logger.debug(() -> format("Idle connection evictor started with wait time: [%s] max idle: [%s]", sleepTime, maxIdleTime));

        cancellableTask.set(threadPool.scheduleWithFixedDelay(() -> {
            try {
                connectionManager.closeExpired();
                if (maxIdleTime.get() != null) {
                    connectionManager.closeIdle(org.apache.hc.core5.util.TimeValue.ofMilliseconds(maxIdleTime.get().millis()));
                }
            } catch (Exception e) {
                logger.warn("HTTP connection eviction failed", e);
            }
        }, sleepTime, threadPool.executor(UTILITY_THREAD_POOL_NAME)));
    }

    @Override
    public void close() {
        if (cancellableTask.get() != null) {
            logger.debug("Idle connection evictor closing");
            cancellableTask.get().cancel();
        }
    }

    public boolean isRunning() {
        return cancellableTask.get() != null && cancellableTask.get().isCancelled() == false;
    }
}
