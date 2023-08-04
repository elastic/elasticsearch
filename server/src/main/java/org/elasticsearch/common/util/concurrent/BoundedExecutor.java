/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.lucene.util.ThreadInterruptedException;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * {@link Executor} implementation that wraps another executor and limits the number of concurrent tasks that are delegated to it.
 * Meant to be used when the caller thread is executed as part of a separate thread pool that is subject to its own queueing and rejection
 * policy. Simplifies queue and rejection handling in the secondary thread pool as it effectively relies on those of the caller thread pool.
 *
 * Not suitable for non-blocking scenarios as it assumes that the caller thread will block and wait for all its tasks to be
 * completed anyway. In this scenario an additional wait when tasks are submitted does not introduce extra overhead.
 *
 * Note: execution permits are released before the delegate executor updates its internal state to mark each task completed.
 * That means that we'll accept tasks before there are free threads to take them, hence there will be some overflow queueing required in
 * the underlying thread pool executor. This is the reason why a queue is required despite the wait applied at submit.
 */
public class BoundedExecutor implements Executor {
    private final Executor executor;
    private final int bound;
    private final Semaphore semaphore;

    public BoundedExecutor(ThreadPoolExecutor executor) {
        this(executor, executor.getMaximumPoolSize());
    }

    public BoundedExecutor(Executor executor, int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound should be positive");
        }
        this.executor = executor;
        this.bound = bound;
        this.semaphore = new Semaphore(bound, true);
    }

    /**
     * Returns the number of tasks that this executor will allow to be executed concurrently
     */
    public final int getBound() {
        return bound;
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command cannot be null");
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
        }
        try {
            executor.execute(() -> {
                try {
                    command.run();
                } finally {
                    semaphore.release();
                }
            });
        } catch (RejectedExecutionException e) {
            semaphore.release();
        }
    }
}
