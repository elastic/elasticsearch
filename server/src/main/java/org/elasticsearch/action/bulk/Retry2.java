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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Encapsulates asynchronous retry logic.
 */
class Retry2 {
    private final Logger logger;
    private final BackoffPolicy backoffPolicy;
    private final Scheduler scheduler;
    private final BlockingQueue<RetryQueuePayload> readyToLoadQueue;
    private final PriorityBlockingQueue<Tuple<Long, RetryQueuePayload>> retryQueue;
    private final int readyToLoadQueueCapacity;
    private final int retryQueueCapacity;
    private final Semaphore requestsInFlightSemaphore;
    private final int maxNumberOfConcurrentRequests;
    private Scheduler.Cancellable flushCancellable;

    Retry2(
        BackoffPolicy backoffPolicy,
        Scheduler scheduler,
        int readyToLoadQueueCapacity,
        int retryQueueCapacity,
        int maxNumberOfConcurrentRequests
    ) {
        this.logger = LogManager.getLogger(getClass());
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
        this.readyToLoadQueueCapacity = readyToLoadQueueCapacity;
        this.retryQueueCapacity = retryQueueCapacity;
        this.maxNumberOfConcurrentRequests = Math.max(maxNumberOfConcurrentRequests, 1);
        requestsInFlightSemaphore = new Semaphore(this.maxNumberOfConcurrentRequests);
        this.readyToLoadQueue = new ArrayBlockingQueue<>(readyToLoadQueueCapacity);
        this.retryQueue = new PriorityBlockingQueue<>(readyToLoadQueueCapacity, Comparator.comparing(Tuple::v1));
    }

    public void init() {
        flushCancellable = scheduler.scheduleWithFixedDelay(this::flush, TimeValue.timeValueMillis(100), ThreadPool.Names.GENERIC);
    }

    private record RetryQueuePayload(
        BulkRequest request,
        List<BulkItemResponse> responses,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        Iterator<TimeValue> backoff,
        boolean isRetry
    ) {}

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
        Iterator<TimeValue> backoff = backoffPolicy.iterator();
        boolean accepted = readyToLoadQueue.offer(
            new RetryQueuePayload(bulkRequest, new ArrayList<>(), consumer, listener, backoff, false)
        );
        if (accepted == false) {
            logger.trace("Rejecting an initial bulk request because the queue is full");
            onFailure(
                bulkRequest,
                new ArrayList<>(),
                consumer,
                listener,
                new EsRejectedExecutionException(
                    "Could not retry bulk request, bulk request queue at capacity ["
                        + readyToLoadQueue.size()
                        + "/"
                        + readyToLoadQueueCapacity
                        + "]"
                ),
                false,
                backoff
            );
        }
    }

    public void onFailure(
        BulkRequest bulkRequest,
        List<BulkItemResponse> responsesAccumulator,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        Exception e,
        boolean retry,
        Iterator<TimeValue> backoff
    ) {
        if (retry) {
            retry(bulkRequest, responsesAccumulator, consumer, listener, backoff);
        } else {
            listener.onFailure(e);
        }
    }

    private void retry(
        BulkRequest bulkRequestForRetry,
        List<BulkItemResponse> responsesAccumulator,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        Iterator<TimeValue> backoff
    ) {
        /*
         * This is not threadsafe, but it's close enough. If we have so many retries that we're anywhere near capacity it is not a bad
         * thing that we start rejecting some.
         */
        if (retryQueue.size() > retryQueueCapacity) {
            logger.trace("Rejecting a retry request because the retry queue is full");
            onFailure(
                bulkRequestForRetry,
                responsesAccumulator,
                consumer,
                listener,
                new EsRejectedExecutionException(
                    "Could not retry bulk request, bulk request queue at capacity ["
                        + readyToLoadQueue.size()
                        + "/"
                        + readyToLoadQueueCapacity
                        + "]"
                ),
                false,
                backoff
            );
        }
        // here calculate when this thing will next be up for retry (in clock time) and put on retryQueue
        TimeValue timeUntilNextRetry = backoff.next();
        long currentTime = System.nanoTime();
        long timeThisRetryMatures = timeUntilNextRetry.nanos() + currentTime;
        retryQueue.offer(
            Tuple.tuple(
                timeThisRetryMatures,
                new RetryQueuePayload(bulkRequestForRetry, responsesAccumulator, consumer, listener, backoff, true)
            )
        );
    }

    void flush() {
        while (true) {
            Tuple<Long, RetryQueuePayload> retry = retryQueue.poll();
            if (retry == null) {
                break;
            }
            if (retry.v1() < System.nanoTime()) {
                logger.trace("Promoting a retry to the readyToLoadQueue");
                RetryQueuePayload retryQueuePayload = retry.v2();
                boolean accepted = readyToLoadQueue.offer(retryQueuePayload);
                if (accepted == false) {
                    onFailure(
                        retryQueuePayload.request,
                        retryQueuePayload.responses,
                        retryQueuePayload.consumer,
                        retryQueuePayload.listener,
                        new EsRejectedExecutionException(
                            "Could not retry bulk request, bulk request queue at capacity ["
                                + readyToLoadQueue.size()
                                + "/"
                                + readyToLoadQueueCapacity
                                + "]"
                        ),
                        true,
                        retryQueuePayload.backoff
                    );
                }
            } else {
                logger.trace("At least one retry pending, but it is not yet time to execute it");
                retryQueue.offer(retry);
                break;
            }
        }
        while (true) {
            boolean allowedToMakeRequest = requestsInFlightSemaphore.tryAcquire();
            if (allowedToMakeRequest == false) {
                logger.trace("Unable to acquire semaphore because too many requests are already in flight");
                /*
                 * Too many requests are already in flight, so don't flush a bulk request to Elasticsearch.
                 */
                return;
            }
            RetryQueuePayload queueItem = readyToLoadQueue.poll();
            if (queueItem == null) {
                requestsInFlightSemaphore.release();
                return;
            }
            logger.trace("Sending a bulk request");
            BulkRequest bulkRequest = queueItem.request;
            List<BulkItemResponse> responsesAccumulator = queueItem.responses;
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = queueItem.consumer;
            ActionListener<BulkResponse> listener = queueItem.listener;
            Iterator<TimeValue> backoff = queueItem.backoff;
            consumer.accept(bulkRequest, new RetryHandler(bulkRequest, responsesAccumulator, consumer, listener, backoff));
        }
    }

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        List<RetryQueuePayload> remainingRequests = new ArrayList<>();
        readyToLoadQueue.drainTo(remainingRequests);
        for (RetryQueuePayload request : remainingRequests) {
            request.listener.onFailure(new EsRejectedExecutionException("Closing the bulk request handler"));
        }
        boolean noRequestsInFlight = requestsInFlightSemaphore.tryAcquire(maxNumberOfConcurrentRequests, timeout, unit);
        flushCancellable.cancel();
        return noRequestsInFlight && readyToLoadQueue.isEmpty();
    }

    private final class RetryHandler extends ActionListener.Delegating<BulkResponse, BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private final BulkRequest bulkRequest;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final List<BulkItemResponse> responsesAccumulator;
        private final long startTimestampNanos;
        private final Iterator<TimeValue> backoff;

        RetryHandler(
            BulkRequest bulkRequest,
            List<BulkItemResponse> responsesAccumulator,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            Iterator<TimeValue> backoff
        ) {
            super(listener);
            this.bulkRequest = bulkRequest;
            this.responsesAccumulator = responsesAccumulator;
            this.consumer = consumer;
            this.startTimestampNanos = System.nanoTime();
            this.backoff = backoff;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            requestsInFlightSemaphore.release();
            if (bulkItemResponses.hasFailures() == false) {
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                finishHim();
            } else {
                if (canRetry(bulkItemResponses)) {
                    addResponses(bulkItemResponses, (r -> r.isFailed() == false));
                    retry(createBulkRequestForRetry(bulkItemResponses), responsesAccumulator, consumer, delegate, backoff);
                } else {
                    addResponses(bulkItemResponses, (r -> true));
                    finishHim();
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            requestsInFlightSemaphore.release();
            boolean retry = ExceptionsHelper.status(e) == RETRY_STATUS && backoff.hasNext();
            Retry2.this.onFailure(this.bulkRequest, responsesAccumulator, consumer, delegate, e, retry, backoff);
        }

        private BulkRequest createBulkRequestForRetry(BulkResponse bulkItemResponses) {
            BulkRequest requestToReissue = new BulkRequest();
            int index = 0;
            for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    DocWriteRequest<?> originalBulkItemRequest = bulkRequest.requests().get(index);
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
                return false;
            }
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                if (bulkItemResponse.isFailed()) {
                    final RestStatus status = bulkItemResponse.status();
                    if (status != RETRY_STATUS) {
                        return false;
                    }
                }
            }
            return true;
        }

        private void finishHim() {
            delegate.onResponse(getAccumulatedResponse());
        }

        private void addResponses(BulkResponse response, Predicate<BulkItemResponse> filter) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (filter.test(bulkItemResponse)) {
                    // Use client-side lock here to avoid visibility issues. This method may be called multiple times
                    // (based on how many retries we have to issue) and relying that the response handling code will be
                    // scheduled on the same thread is fragile.
                    synchronized (responsesAccumulator) {
                        responsesAccumulator.add(bulkItemResponse);
                    }
                }
            }
        }

        private BulkResponse getAccumulatedResponse() {
            BulkItemResponse[] itemResponses;
            synchronized (responsesAccumulator) {
                itemResponses = responsesAccumulator.toArray(new BulkItemResponse[0]);
            }
            long stopTimestamp = System.nanoTime();
            long totalLatencyMs = TimeValue.timeValueNanos(stopTimestamp - startTimestampNanos).millis();
            return new BulkResponse(itemResponses, totalLatencyMs);
        }
    }
}
