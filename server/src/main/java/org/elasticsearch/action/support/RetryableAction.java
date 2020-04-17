/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.TimeValue;
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
    private final String retryExecutor;

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
        this.timeoutMillis = Math.max(timeoutValue.getMillis(), 1);
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
        this.retryExecutor = executor;
    }

    public void run() {
        final RetryingListener retryingListener = new RetryingListener(initialDelayMillis, null);
        final Runnable runnable = createRunnable(retryingListener);
        threadPool.executor(retryExecutor).execute(runnable);
    }

    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            finalListener.onFailure(e);
        }

    }

    private Runnable createRunnable(RetryingListener retryingListener) {
        return new ActionRunnable<>(retryingListener) {

            @Override
            protected void doRun() {
                tryAction(listener);
            }

            @Override
            public void onRejection(Exception e) {
                // Immediately fail because we were not able to schedule the action
                retryingListener.addException(e);
                finalListener.onFailure(retryingListener.buildFinalException());
            }
        };
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private final long nextDelayMillis;
        private ArrayDeque<Exception> caughtExceptions;

        private RetryingListener(long nextDelayMillis, ArrayDeque<Exception> caughtExceptions) {
            this.nextDelayMillis = nextDelayMillis;
            this.caughtExceptions = caughtExceptions;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
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
                    addException(e);
                    if (isDone.compareAndSet(false, true)) {
                        finalListener.onFailure(buildFinalException());
                    }
                } else {
                    logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}",
                        TimeValue.timeValueMillis(nextDelayMillis)), e);
                    addException(e);

                    final RetryingListener retryingListener = new RetryingListener(nextDelayMillis * 2, caughtExceptions);
                    final Runnable runnable = createRunnable(retryingListener);
                    final long midpoint = (nextDelayMillis / 2);
                    final int randomness = Randomness.get().nextInt((int) Math.min(midpoint, Integer.MAX_VALUE));
                    final long delayMillis = midpoint + randomness;
                    if (isDone.get() == false) {
                        threadPool.schedule(runnable, TimeValue.timeValueMillis(delayMillis), retryExecutor);
                    }
                }
            } else {
                addException(e);
                if (isDone.compareAndSet(false,true)) {
                    finalListener.onFailure(buildFinalException());
                }
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
