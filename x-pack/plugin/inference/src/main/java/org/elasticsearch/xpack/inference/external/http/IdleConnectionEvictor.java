/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Starts a monitoring task to remove expired and idle connections from the HTTP connection pool.
 * This is modeled off of https://github.com/apache/httpcomponents-client/blob/master/httpclient5/
 * src/main/java/org/apache/hc/client5/http/impl/IdleConnectionEvictor.java
 *
 * NOTE: This class should be removed once the apache async client is upgraded to 5.x because that version of the library
 * includes this already.
 *
 * See <a href="https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/connmgmt.html#d5e418">here for more info.</a>
 */
public class IdleConnectionEvictor implements Closeable {
    private static final Logger logger = LogManager.getLogger(IdleConnectionEvictor.class);

    private final ThreadPool threadPool;
    private final NHttpClientConnectionManager connectionManager;
    private final TimeValue sleepTime;
    private final AtomicReference<TimeValue> maxIdleTime = new AtomicReference<>();
    private final AtomicReference<Scheduler.Cancellable> cancellableTask = new AtomicReference<>();

    public IdleConnectionEvictor(
        ThreadPool threadPool,
        NHttpClientConnectionManager connectionManager,
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
                connectionManager.closeExpiredConnections();
                if (maxIdleTime.get() != null) {
                    connectionManager.closeIdleConnections(maxIdleTime.get().millis(), TimeUnit.MILLISECONDS);
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
