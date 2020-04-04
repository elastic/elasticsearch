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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

public abstract class RetryableAction<Response> {

    private final Logger logger;

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
                           ActionListener<Response> listener, String retryExecutor) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.initialDelayMillis = Math.max(initialDelay.getMillis(), 1);
        this.timeoutMillis = Math.max(timeoutValue.getMillis(), 1);
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
        this.retryExecutor = retryExecutor;
    }

    public void run() {
        tryAction(new RetryingListener(initialDelayMillis, null));
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    private class RetryingListener implements ActionListener<Response> {

        private final long nextDelayMillis;
        private final Exception existingException;

        private RetryingListener(long nextDelayMillis, Exception existingException) {
            this.nextDelayMillis = nextDelayMillis;
            this.existingException = existingException;
        }

        @Override
        public void onResponse(Response response) {
            finalListener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            if (shouldRetry(e)) {
                final long elapsedMillis = threadPool.relativeTimeInMillis() - startMillis;
                if (elapsedMillis > timeoutMillis) {
                    logger.debug(() -> new ParameterizedMessage("retryable action timed out after {}",
                        TimeValue.timeValueMillis(elapsedMillis)), e);
                    addExisting(e);
                    finalListener.onFailure(e);
                } else {
                    logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}",
                        TimeValue.timeValueMillis(nextDelayMillis)), e);
                    addExisting(e);
                    Runnable runnable = () -> tryAction(new RetryingListener(nextDelayMillis * 2, e));
                    final long midpoint = (nextDelayMillis / 2);
                    final int randomness = Randomness.get().nextInt((int) Math.min(midpoint, Integer.MAX_VALUE));
                    final long delayMillis = midpoint + randomness;
                    threadPool.schedule(runnable, TimeValue.timeValueMillis(delayMillis), retryExecutor);
                }
            } else {
                addExisting(e);
                finalListener.onFailure(e);
            }
        }

        private void addExisting(Exception e) {
            if (existingException != null) {
                e.addSuppressed(existingException);
            }
        }
    }
}
