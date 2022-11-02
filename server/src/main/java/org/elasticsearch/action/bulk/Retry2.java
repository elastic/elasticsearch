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
import java.util.stream.StreamSupport;

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
    private boolean isClosing = false;
    private long closingTime = -1;

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
        this.requestsInFlightSemaphore = new Semaphore(this.maxNumberOfConcurrentRequests);
        /*
         * Note that the capacity for ArrayBlockingQueue is a firm capacity, and attempts to add more than that many things will get
         * rejected. But the capacity for PriorityBlockingQueue is just an initial capacity. The queue itself is unbounded. So we enforce
         *  this capacity in the code below.
         */
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
        Iterator<TimeValue> backoff
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
        boolean accepted = readyToLoadQueue.offer(new RetryQueuePayload(bulkRequest, new ArrayList<>(), consumer, listener, backoff));
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
        /*
         * Here calculate when this request will next be up for retry (in clock time) and put it on retryQueue. We use nanonTime rather
         * than currentTimeInMillis because nanoTime will not go backwards within a single JVM. We do not actually care about
         * nanosecond-level resolution.
         */
        TimeValue timeUntilNextRetry = backoff.next();
        long currentTime = System.nanoTime();
        long timeThisRetryMatures = timeUntilNextRetry.nanos() + currentTime;
        logger.trace("Queueing a retry to start after {}", timeUntilNextRetry);
        retryQueue.offer(
            Tuple.tuple(timeThisRetryMatures, new RetryQueuePayload(bulkRequestForRetry, responsesAccumulator, consumer, listener, backoff))
        );
    }

    void flush() {
        while (isClosing == false || System.nanoTime() < closingTime) {
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
        while (isClosing == false || System.nanoTime() < closingTime) {
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
            consumer.accept(
                bulkRequest,
                new RetryHandler(requestsInFlightSemaphore, bulkRequest, responsesAccumulator, consumer, listener, backoff)
            );
        }
    }

    void awaitClose(long timeout, TimeUnit unit) {
        logger.trace("Starting awaitClose");
        isClosing = true;
        TimeValue remainingTime = new TimeValue(timeout, unit);
        closingTime = System.nanoTime() + remainingTime.getNanos();
        flushCancellable.cancel();
        /*
         * The following flush will run at most until closingTime. After it completes, anything that remains is something that we didn't
         * have time to get to, or was added later.
         */
        flush();
        List<RetryQueuePayload> remainingReadyRequests = new ArrayList<>();
        readyToLoadQueue.drainTo(remainingReadyRequests);
        for (RetryQueuePayload request : remainingReadyRequests) {
            request.listener.onFailure(new EsRejectedExecutionException("Closing the bulk request handler"));
        }
        List<Tuple<Long, RetryQueuePayload>> remainingRetryRequests = new ArrayList<>();
        retryQueue.drainTo(remainingRetryRequests);
        for (Tuple<Long, RetryQueuePayload> retry : remainingRetryRequests) {
            retry.v2().listener.onFailure(new EsRejectedExecutionException("Closing the bulk request handler"));
        }
        logger.trace("Finishing awaitClose");
    }

    private final class RetryHandler implements ActionListener<BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private final Semaphore requestsInFlightSemaphore;
        private final BulkRequest bulkRequest;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final ActionListener<BulkResponse> listener;
        private final List<BulkItemResponse> responsesAccumulator;
        private final long startTimestampNanos;
        private final Iterator<TimeValue> backoff;

        RetryHandler(
            Semaphore requestsInFlightSemaphore,
            BulkRequest bulkRequest,
            List<BulkItemResponse> responsesAccumulator,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            Iterator<TimeValue> backoff
        ) {
            this.requestsInFlightSemaphore = requestsInFlightSemaphore;
            this.bulkRequest = bulkRequest;
            this.responsesAccumulator = responsesAccumulator;
            this.consumer = consumer;
            this.listener = listener;
            this.startTimestampNanos = System.nanoTime();
            this.backoff = backoff;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            requestsInFlightSemaphore.release();
            if (bulkItemResponses.hasFailures() == false) {
                logger.trace(
                    "Got a response in {} with {} items, no failures",
                    bulkItemResponses.getTook(),
                    bulkItemResponses.getItems().length
                );
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                listener.onResponse(getAccumulatedResponse());
            } else {
                if (canRetry(bulkItemResponses)) {
                    logger.trace(
                        "Got a response in {} with {} items including failures, can retry",
                        bulkItemResponses.getTook(),
                        bulkItemResponses.getItems().length
                    );
                    addResponses(bulkItemResponses, (r -> r.isFailed() == false));
                    retry(createBulkRequestForRetry(bulkItemResponses), responsesAccumulator, consumer, listener, backoff);
                } else {
                    logger.trace(
                        "Got a response in {} with {} items including failures, cannot retry",
                        bulkItemResponses.getTook(),
                        bulkItemResponses.getItems().length
                    );
                    addResponses(bulkItemResponses, (r -> true));
                    listener.onResponse(getAccumulatedResponse());
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            requestsInFlightSemaphore.release();
            boolean retry = ExceptionsHelper.status(e) == RETRY_STATUS && backoff.hasNext();
            Retry2.this.onFailure(this.bulkRequest, responsesAccumulator, consumer, listener, e, retry, backoff);
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

        private void addResponses(BulkResponse response, Predicate<BulkItemResponse> filter) {
            List<BulkItemResponse> bulkItemResponses = StreamSupport.stream(response.spliterator(), false).filter(filter).toList();
            responsesAccumulator.addAll(bulkItemResponses);
        }

        private BulkResponse getAccumulatedResponse() {
            BulkItemResponse[] itemResponses = responsesAccumulator.toArray(new BulkItemResponse[0]);
            long stopTimestamp = System.nanoTime();
            long totalLatencyMs = TimeValue.timeValueNanos(stopTimestamp - startTimestampNanos).millis();
            logger.trace("Accumulated response includes {} items", itemResponses.length);
            return new BulkResponse(itemResponses, totalLatencyMs);
        }
    }
}
