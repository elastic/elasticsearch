/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Encapsulates synchronous and asynchronous retry logic.
 */
public class Retry {
    private final Class<? extends Throwable> retryOnThrowable;
    private final BackoffPolicy backoffPolicy;
    private final Scheduler scheduler;

    public Retry(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy, Scheduler scheduler) {
        this.retryOnThrowable = retryOnThrowable;
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception and delegates results to the
     * provided listener. Retries will be scheduled using the class's thread pool.
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @param listener A listener that is invoked when the bulk request finishes or completes with an exception. The listener is not
     * @param settings settings
     */
    public void withBackoff(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BulkRequest bulkRequest,
                            ActionListener<BulkResponse> listener, Settings settings) {
        RetryHandler r = new RetryHandler(retryOnThrowable, backoffPolicy, consumer, listener, settings, scheduler);
        r.execute(bulkRequest);
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception. Retries will be scheduled using
     * the class's thread pool.
     *
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @param settings settings
     * @return a future representing the bulk response returned by the client.
     */
    public PlainActionFuture<BulkResponse> withBackoff(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
                                                       BulkRequest bulkRequest, Settings settings) {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        withBackoff(consumer, bulkRequest, future, settings);
        return future;
    }

    static class RetryHandler implements ActionListener<BulkResponse> {
        private final Logger logger;
        private final Scheduler scheduler;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final ActionListener<BulkResponse> listener;
        private final Iterator<TimeValue> backoff;
        private final Class<? extends Throwable> retryOnThrowable;
        // Access only when holding a client-side lock, see also #addResponses()
        private final List<BulkItemResponse> responses = new ArrayList<>();
        private final long startTimestampNanos;
        // needed to construct the next bulk request based on the response to the previous one
        // volatile as we're called from a scheduled thread
        private volatile BulkRequest currentBulkRequest;
        private volatile ScheduledFuture<?> scheduledRequestFuture;

        RetryHandler(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy,
                     BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, ActionListener<BulkResponse> listener,
                     Settings settings, Scheduler scheduler) {
            this.retryOnThrowable = retryOnThrowable;
            this.backoff = backoffPolicy.iterator();
            this.consumer = consumer;
            this.listener = listener;
            this.logger = Loggers.getLogger(getClass(), settings);
            this.scheduler = scheduler;
            // in contrast to System.currentTimeMillis(), nanoTime() uses a monotonic clock under the hood
            this.startTimestampNanos = System.nanoTime();
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (!bulkItemResponses.hasFailures()) {
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                finishHim();
            } else {
                if (canRetry(bulkItemResponses)) {
                    addResponses(bulkItemResponses, (r -> !r.isFailed()));
                    retry(createBulkRequestForRetry(bulkItemResponses));
                } else {
                    addResponses(bulkItemResponses, (r -> true));
                    finishHim();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                listener.onFailure(e);
            } finally {
                FutureUtils.cancel(scheduledRequestFuture);
            }
        }

        private void retry(BulkRequest bulkRequestForRetry) {
            assert backoff.hasNext();
            TimeValue next = backoff.next();
            logger.trace("Retry of bulk request scheduled in {} ms.", next.millis());
            Runnable command = scheduler.preserveContext(() -> this.execute(bulkRequestForRetry));
            scheduledRequestFuture = scheduler.schedule(next, ThreadPool.Names.SAME, command);
        }

        private BulkRequest createBulkRequestForRetry(BulkResponse bulkItemResponses) {
            BulkRequest requestToReissue = new BulkRequest();
            int index = 0;
            for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    requestToReissue.add(currentBulkRequest.requests().get(index));
                }
                index++;
            }
            return requestToReissue;
        }

        private boolean canRetry(BulkResponse bulkItemResponses) {
            if (!backoff.hasNext()) {
                return false;
            }
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                if (bulkItemResponse.isFailed()) {
                    final Throwable cause = bulkItemResponse.getFailure().getCause();
                    final Throwable rootCause = ExceptionsHelper.unwrapCause(cause);
                    if (!rootCause.getClass().equals(retryOnThrowable)) {
                        return false;
                    }
                }
            }
            return true;
        }

        private void finishHim() {
            try {
                listener.onResponse(getAccumulatedResponse());
            } finally {
                FutureUtils.cancel(scheduledRequestFuture);
            }
        }

        private void addResponses(BulkResponse response, Predicate<BulkItemResponse> filter) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (filter.test(bulkItemResponse)) {
                    // Use client-side lock here to avoid visibility issues. This method may be called multiple times
                    // (based on how many retries we have to issue) and relying that the response handling code will be
                    // scheduled on the same thread is fragile.
                    synchronized (responses) {
                        responses.add(bulkItemResponse);
                    }
                }
            }
        }

        private BulkResponse getAccumulatedResponse() {
            BulkItemResponse[] itemResponses;
            synchronized (responses) {
                itemResponses = responses.toArray(new BulkItemResponse[1]);
            }
            long stopTimestamp = System.nanoTime();
            long totalLatencyMs = TimeValue.timeValueNanos(stopTimestamp - startTimestampNanos).millis();
            return new BulkResponse(itemResponses, totalLatencyMs);
        }

        public void execute(BulkRequest bulkRequest) {
            this.currentBulkRequest = bulkRequest;
            consumer.accept(bulkRequest, this);
        }
    }
}
