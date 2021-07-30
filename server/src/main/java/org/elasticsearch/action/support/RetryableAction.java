/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A action that will be retried on failure if {@link RetryableAction#shouldRetry(Exception)} returns true.
 * The executor the action will be executed on can be defined in the constructor. Otherwise, SAME is the
 * default. The action will be retried with exponentially increasing delay periods until the timeout period
 * has been reached.
 */
public abstract class RetryableAction<Response> {

    private final Logger logger;

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private final long initialDelayMillis;
    private final long timeoutMillis;
    private final long startMillis;
    private final ActionListener<Response> finalListener;
    private final String executor;

    private volatile Scheduler.ScheduledCancellable retryTask;

    public RetryableAction(Logger logger, ThreadPool threadPool, TimeValue initialDelay, TimeValue timeoutValue,
                           ActionListener<Response> listener) {
        this(logger, threadPool, initialDelay, timeoutValue, listener, ThreadPool.Names.SAME);
    }

    public RetryableAction(Logger logger, ThreadPool threadPool, TimeValue initialDelay, TimeValue timeoutValue,
                           ActionListener<Response> listener, String executor) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.initialDelayMillis = initialDelay.getMillis();
        if (initialDelayMillis < 1) {
            throw new IllegalArgumentException("Initial delay was less than 1 millisecond: " + initialDelay);
        }
        this.timeoutMillis = timeoutValue.getMillis();
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
        this.executor = executor;
    }

    public void run() {
        final RetryingListener retryingListener = new RetryingListener(initialDelayMillis, null);
        final Runnable runnable = createRunnable(retryingListener);
        threadPool.executor(executor).execute(runnable);
    }

    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            Scheduler.ScheduledCancellable localRetryTask = this.retryTask;
            if (localRetryTask != null) {
                localRetryTask.cancel();
            }
            onFinished();
            finalListener.onFailure(e);
        }
    }

    private Runnable createRunnable(RetryingListener retryingListener) {
        return new ActionRunnable<>(retryingListener) {

            @Override
            protected void doRun() {
                retryTask = null;
                // It is possible that the task was cancelled in between the retry being dispatched and now
                if (isDone.get() == false) {
                    tryAction(listener);
                }
            }

            @Override
            public void onRejection(Exception e) {
                retryTask = null;
                // TODO: The only implementations of this class use SAME which means the execution will not be
                //  rejected. Future implementations can adjust this functionality as needed.
                onFailure(e);
            }
        };
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    protected long calculateDelay(long previousDelay) {
        return Math.min(previousDelay * 2, Integer.MAX_VALUE);
    }

    protected long minimumDelayMillis() {
        return 1L;
    }

    public void onFinished() {
    }

    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private final long delayMillisBound;
        private ArrayDeque<Exception> caughtExceptions;

        private RetryingListener(long delayMillisBound, ArrayDeque<Exception> caughtExceptions) {
            this.delayMillisBound = delayMillisBound;
            this.caughtExceptions = caughtExceptions;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (shouldRetry(e)) {
                final long elapsedMillis = threadPool.relativeTimeInMillis() - startMillis;
                if (elapsedMillis >= timeoutMillis) {
                    logger.debug(() -> new ParameterizedMessage("retryable action timed out after {}",
                        TimeValue.timeValueMillis(elapsedMillis)), e);
                    onFinalFailure(e);
                } else {
                    addException(e);

                    final long nextDelayMillisBound = calculateDelay(delayMillisBound);
                    final RetryingListener retryingListener = new RetryingListener(nextDelayMillisBound, caughtExceptions);
                    final Runnable runnable = createRunnable(retryingListener);
                    final long delayMillis = Randomness.get().nextInt(Math.toIntExact(delayMillisBound)) + minimumDelayMillis();
                    if (isDone.get() == false) {
                        final TimeValue delay = TimeValue.timeValueMillis(delayMillis);
                        logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}", delay), e);
                        try {
                            retryTask = threadPool.schedule(runnable, delay, executor);
                        } catch (EsRejectedExecutionException ree) {
                            onFinalFailure(ree);
                        }
                    }
                }
            } else {
                onFinalFailure(e);
            }
        }

        private void onFinalFailure(Exception e) {
            addException(e);
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onFailure(buildFinalException());
            }
        }

        private Exception buildFinalException() {
            final Exception topLevel = caughtExceptions.removeFirst();
            Exception suppressed;
            while ((suppressed = caughtExceptions.pollFirst()) != null) {
                topLevel.addSuppressed(suppressed);
            }
            return topLevel;
        }

        private void addException(Exception e) {
            if (caughtExceptions != null) {
                if (caughtExceptions.size() == MAX_EXCEPTIONS) {
                    caughtExceptions.removeLast();
                }
            } else {
                caughtExceptions = new ArrayDeque<>(MAX_EXCEPTIONS);
            }
            caughtExceptions.addFirst(e);
        }
    }
}
