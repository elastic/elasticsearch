/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Encapsulates synchronous and asynchronous retry logic.
 */
public class Retry {
    private final BackoffPolicy backoffPolicy;
    private final Scheduler scheduler;
    private final int maxQueueSize;
    private final AtomicInteger queueSize = new AtomicInteger(0);

    public Retry(BackoffPolicy backoffPolicy, Scheduler scheduler, int maxQueueSize) {
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
        this.maxQueueSize = maxQueueSize;
    }

    public Retry(BackoffPolicy backoffPolicy, Scheduler scheduler) {
        this(backoffPolicy, scheduler, Integer.MAX_VALUE);
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception and delegates results to the
     * provided listener. Retries will be scheduled using the class's thread pool.
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @param listener A listener that is invoked when the bulk request finishes or completes with an exception. The listener is not
     */
    public void withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest,
        ActionListener<BulkResponse> listener
    ) {
        RetryHandler r = new RetryHandler(backoffPolicy, consumer, listener, scheduler, queueSize, maxQueueSize);
        r.execute(bulkRequest);
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception. Retries will be scheduled using
     * the class's thread pool.
     *
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @return a future representing the bulk response returned by the client.
     */
    public PlainActionFuture<BulkResponse> withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest
    ) {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        withBackoff(consumer, bulkRequest, future);
        return future;
    }

    static class RetryHandler extends ActionListener.Delegating<BulkResponse, BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private static final Logger logger = LogManager.getLogger(RetryHandler.class);

        private final Scheduler scheduler;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final Iterator<TimeValue> backoff;
        // Access only when holding a client-side lock, see also #addResponses()
        private final List<BulkItemResponse> responses = new ArrayList<>();
        private final long startTimestampNanos;
        // needed to construct the next bulk request based on the response to the previous one
        // volatile as we're called from a scheduled thread
        private volatile BulkRequest currentBulkRequest;
        private volatile Scheduler.Cancellable retryCancellable;
        private final AtomicInteger queueSize;
        private final int maxQueueSize;

        RetryHandler(
            BackoffPolicy backoffPolicy,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            Scheduler scheduler,
            AtomicInteger queueSize,
            int maxQueueSize
        ) {
            super(listener);
            this.backoff = backoffPolicy.iterator();
            this.consumer = consumer;
            this.scheduler = scheduler;
            // in contrast to System.currentTimeMillis(), nanoTime() uses a monotonic clock under the hood
            this.startTimestampNanos = System.nanoTime();
            this.queueSize = queueSize;
            this.maxQueueSize = maxQueueSize;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (bulkItemResponses.hasFailures() == false) {
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                finishHim();
            } else {
                if (canRetry(bulkItemResponses)) {
                    addResponses(bulkItemResponses, (r -> r.isFailed() == false));
                    retry(createBulkRequestForRetry(bulkItemResponses));
                } else {
                    addResponses(bulkItemResponses, (r -> true));
                    finishHim();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (ExceptionsHelper.status(e) == RETRY_STATUS && backoff.hasNext()) {
                retry(currentBulkRequest);
            } else {
                try {
                    super.onFailure(e);
                } finally {
                    if (retryCancellable != null) {
                        retryCancellable.cancel();
                    }
                }
            }
        }

        private void retry(BulkRequest bulkRequestForRetry) {
            assert backoff.hasNext();
            TimeValue next = backoff.next();
            int newQueueSize = queueSize.incrementAndGet();
            logger.trace("New queue size: " + newQueueSize);
            if (newQueueSize > maxQueueSize) {
                queueSize.decrementAndGet();
                logger.trace("Max queue size of {} exceeded, rejecting entry", maxQueueSize);
                throw new EsRejectedExecutionException("Queue too big");
            }
            logger.trace("Retry of bulk request scheduled in {} ms.", next.millis());
            retryCancellable = scheduler.schedule(() -> {
                logger.trace("Retry beginning");
                this.execute(bulkRequestForRetry);
                queueSize.decrementAndGet();
            }, next, ThreadPool.Names.SAME);
        }

        private BulkRequest createBulkRequestForRetry(BulkResponse bulkItemResponses) {
            BulkRequest requestToReissue = new BulkRequest();
            int index = 0;
            for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    DocWriteRequest<?> originalBulkItemRequest = currentBulkRequest.requests().get(index);
                    if (originalBulkItemRequest instanceof IndexRequest item) {
                        item.reset();
                    }
                    requestToReissue.add(originalBulkItemRequest);
                }
                index++;
            }
            return requestToReissue;
        }

        private boolean canRetry(BulkResponse bulkItemResponses) {
            if (backoff.hasNext() == false) {
                logger.trace("Cannot retry because backoff.hasNext returned false");
                return false;
            }
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                if (bulkItemResponse.isFailed()) {
                    final RestStatus status = bulkItemResponse.status();
                    if (status != RETRY_STATUS) {
                        logger.trace("Cannot retry because status is {}", status);
                        return false;
                    }
                }
            }
            logger.trace("Can retry");
            return true;
        }

        private void finishHim() {
            try {
                delegate.onResponse(getAccumulatedResponse());
            } finally {
                if (retryCancellable != null) {
                    retryCancellable.cancel();
                }
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
                itemResponses = responses.toArray(new BulkItemResponse[0]);
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
