/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * A worker service that executes runnables sequentially in
 * a single worker thread.
 * @param <T> implements Runnable
 */
public abstract class AbstractProcessWorkerExecutorService<T extends Runnable> extends AbstractExecutorService {

    private static final Logger logger = LogManager.getLogger(AbstractProcessWorkerExecutorService.class);

    protected final ThreadContext contextHolder;
    protected final String processName;
    private final CountDownLatch awaitTermination = new CountDownLatch(1);
    protected final BlockingQueue<T> queue;
    private final AtomicReference<Exception> error = new AtomicReference<>();

    private volatile boolean running = true;

    /**
     * @param contextHolder the thread context holder
     * @param processName the name of the process to be used in logging
     * @param queueCapacity the capacity of the queue holding operations. If an operation is added
     *                  for execution when the queue is full a 429 error is thrown.
     * @param queueSupplier BlockingQueue constructor
     */
    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public AbstractProcessWorkerExecutorService(
        ThreadContext contextHolder,
        String processName,
        int queueCapacity,
        Function<Integer, BlockingQueue<T>> queueSupplier
    ) {
        this.contextHolder = Objects.requireNonNull(contextHolder);
        this.processName = Objects.requireNonNull(processName);
        this.queue = queueSupplier.apply(queueCapacity);
    }

    public int queueSize() {
        return queue.size();
    }

    public void shutdownWithError(Exception e) {
        error.set(e);
        shutdown();
    }

    @Override
    public void shutdown() {
        running = false;
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public boolean isShutdown() {
        return running == false;
    }

    @Override
    public boolean isTerminated() {
        return awaitTermination.getCount() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return awaitTermination.await(timeout, unit);
    }

    public void start() {
        try {
            while (running) {
                Runnable runnable = queue.poll(500, TimeUnit.MILLISECONDS);
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        logger.error(() -> "error handling process [" + processName + "] operation", e);
                    }
                    EsExecutors.rethrowErrors(ThreadContext.unwrap(runnable));
                }
            }

            notifyQueueRunnables();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            awaitTermination.countDown();
        }
    }

    protected synchronized void notifyQueueRunnables() {
        if (queue.isEmpty() == false) {
            List<Runnable> notExecuted = new ArrayList<>();
            queue.drainTo(notExecuted);

            String msg = "unable to process as " + processName + " worker service has shutdown";
            Exception ex = error.get();
            for (Runnable runnable : notExecuted) {
                if (runnable instanceof AbstractRunnable ar) {
                    if (ex != null) {
                        ar.onFailure(ex);
                    } else {
                        ar.onRejection(new EsRejectedExecutionException(msg, true));
                    }
                }
            }
        }
    }
}
