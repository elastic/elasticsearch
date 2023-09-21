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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * Starts a monitoring thread to remove expired and idle connections from the HTTP connection pool.
 * This is modeled off of <a href="https://github.com/apache/httpcomponents-client/blob/master/httpclient5/src/main/java/org/apache/hc/client5/http/impl/IdleConnectionEvictor.java">the code here</a>.
 *
 * NOTE: This class should be removed once the apache async client is upgraded to 5.x because that version of the library
 * includes this already.
 *
 * See <a href="https://hc.apache.org/httpcomponents-client-4.5.x/current/tutorial/html/connmgmt.html#d5e418">here for more info.</a>
 */
public class IdleConnectionEvictor {
    private static final Logger logger = LogManager.getLogger(IdleConnectionEvictor.class);

    private final ThreadPool threadPool;
    private final NHttpClientConnectionManager connectionManager;
    // private final Thread thread;
    private final TimeValue sleepTime;
    private final TimeValue maxIdleTime;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Future<?> threadFuture;

    public IdleConnectionEvictor(
        ThreadPool threadPool,
        NHttpClientConnectionManager connectionManager,
        TimeValue sleepTime,
        TimeValue maxIdleTime
    ) {
        this.threadPool = threadPool;
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.sleepTime = sleepTime;
        this.maxIdleTime = maxIdleTime;
    }

    // public IdleConnectionEvictor(
    // final NHttpClientConnectionManager connectionManager,
    // final ThreadFactory threadFactory,
    // final long sleepTime,
    // final TimeUnit sleepTimeUnit,
    // final long maxIdleTime,
    // final TimeUnit maxIdleTimeUnit
    // ) {
    // this.connectionManager = Objects.requireNonNull(connectionManager);
    // this.threadFactory = threadFactory != null ? threadFactory : new DefaultThreadFactory();
    // this.sleepTimeMs = sleepTimeUnit != null ? sleepTimeUnit.toMillis(sleepTime) : sleepTime;
    // this.maxIdleTimeMs = maxIdleTimeUnit != null ? maxIdleTimeUnit.toMillis(maxIdleTime) : maxIdleTime;
    // this.thread = this.threadFactory.newThread(() -> {
    // try {
    // while (Thread.currentThread().isInterrupted() == false) {
    // Thread.sleep(sleepTimeMs);
    // connectionManager.closeExpiredConnections();
    // if (maxIdleTimeMs > 0) {
    // connectionManager.closeIdleConnections(maxIdleTimeMs, TimeUnit.MILLISECONDS);
    // }
    // }
    // } catch (final Exception ex) {
    // exception = ex;
    // }
    //
    // });
    // }
    //
    // public IdleConnectionEvictor(
    // final HttpClientConnectionManager connectionManager,
    // final long sleepTime,
    // final TimeUnit sleepTimeUnit,
    // final long maxIdleTime,
    // final TimeUnit maxIdleTimeUnit
    // ) {
    // this(connectionManager, null, sleepTime, sleepTimeUnit, maxIdleTime, maxIdleTimeUnit);
    // }
    //
    // public IdleConnectionEvictor(
    // final HttpClientConnectionManager connectionManager,
    // final long maxIdleTime,
    // final TimeUnit maxIdleTimeUnit
    // ) {
    // this(
    // connectionManager,
    // null,
    // maxIdleTime > 0 ? maxIdleTime : 5,
    // maxIdleTimeUnit != null ? maxIdleTimeUnit : TimeUnit.SECONDS,
    // maxIdleTime,
    // maxIdleTimeUnit
    // );
    // }

    public void start() {
        // thread.start();
        if (running.compareAndSet(false, true)) {
            startInternal();
        }
    }

    private void startInternal() {
        threadFuture = threadPool.executor(UTILITY_THREAD_POOL_NAME).submit(() -> {
            logger.debug("HTTP connection eviction thread starting");
            try {
                // while (Thread.currentThread().isInterrupted() == false) {
                while (running.get()) {
                    Thread.sleep(sleepTime.millis());
                    connectionManager.closeExpiredConnections();
                    if (maxIdleTime != null) {
                        connectionManager.closeIdleConnections(maxIdleTime.millis(), TimeUnit.MILLISECONDS);
                    }
                }
            } catch (Exception e) {
                logger.error("HTTP connection eviction thread failed", e);
            } finally {
                running.set(false);
            }

            logger.debug("HTTP connection eviction thread stopped");
        });

    }

    public void shutdown() {
        // thread.interrupt();
        running.set(false);
    }

    public void shutdownNow() {
        shutdown();
        if (threadFuture != null) {
            threadFuture.cancel(true);
        }
    }

    public boolean isRunning() {
        // return thread.isAlive();
        return running.get();
    }

    // public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    public void awaitTermination(long timeout, TimeUnit unit) throws TimeoutException {
        if (threadFuture == null) {
            return;
        }

        // thread.join(timeout.millis());
        try {
            threadFuture.get(timeout, unit);
        } catch (CancellationException | InterruptedException | ExecutionException e) {
            // ignore
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(final Runnable r) {
            final Thread t = new Thread(r, "Connection evictor");
            t.setDaemon(true);
            return t;
        }

    };
}
