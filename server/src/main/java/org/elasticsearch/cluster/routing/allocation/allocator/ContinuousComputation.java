/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronously runs some computation using at most one thread but expects the input value changes over time as it's running. Newer input
 * values are assumed to be fresher and trigger a recomputation. If a computation never starts before a fresher value arrives then it is
 * skipped.
 */
public abstract class ContinuousComputation<T> {

    private static final Logger logger = LogManager.getLogger(ContinuousComputation.class);

    private final ExecutorService executorService;
    private final AtomicReference<T> enqueuedInput = new AtomicReference<>();
    private final Processor processor = new Processor();

    /**
     * @param threadPool Each computation runs on a {@code GENERIC} thread from this thread pool. At most one task executes at once.
     */
    public ContinuousComputation(ThreadPool threadPool) {
        this.executorService = threadPool.generic();
    }

    /**
     * Called when the input value has changed. If no newer value is received then eventually either the computation will run on this value.
     */
    public void onNewInput(T input) {
        assert input != null;
        if (enqueuedInput.getAndSet(Objects.requireNonNull(input)) == null) {
            executorService.execute(processor);
        }
    }

    /**
     * @return {@code false} iff there are no active/enqueued computations
     */
    // exposed for tests
    boolean isActive() {
        return enqueuedInput.get() != null;
    }

    /**
     * @return {@code true} iff the given {@code input} is the latest known input.
     */
    protected boolean isFresh(T input) {
        return enqueuedInput.get() == input;
    }

    /**
     * Process the given input.
     *
     * @param input the value that was last received by {@link #onNewInput} before invocation.
     */
    protected abstract void processInput(T input);

    private class Processor extends AbstractRunnable {

        @Override
        public void onFailure(Exception e) {
            assert false : e;
        }

        @Override
        public void onRejection(Exception e) {
            // The executor has an unbounded queue so we must be shutting down to get here.
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            logger.debug("rejected", e);
        }

        @Override
        protected void doRun() throws Exception {
            final T input = enqueuedInput.get();
            assert input != null;

            processInput(input);

            if (enqueuedInput.compareAndSet(input, null) == false) {
                executorService.execute(this);
            }
        }

        @Override
        public String toString() {
            return "ContinuousComputation$Processor[" + ContinuousComputation.this + "]";
        }
    }
}
