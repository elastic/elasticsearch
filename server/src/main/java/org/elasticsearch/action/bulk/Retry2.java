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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Encapsulates asynchronous retry logic. This class maintains two queues: (1) readyToLoadQueue -- this is a queue containing
 * BulkRequests that it can load right now and (2) retryQueue -- these are BulkRequests that are to be loaded at some point in the future.
 */
class Retry2 {
    private final Logger logger;
    private final int maxNumberOfRetries;
    /**
     * This is the scheduler on which we periodically schedule the task that manages the queues.
     */
    private final Scheduler scheduler;
    /**
     * This is the queue of BulkRequests (and their related state) that are ready to be loaded as soon as possible.
     */
    private final BlockingQueue<RetryQueuePayload> readyToLoadQueue;
    private final TimeValue queuePollingInterval;
    /**
     * This is the maximum number of items that can be placed on the readyToLoadQueue. If we attempt to add a BulkRequest after this number
     * is reached, the listener is notified with an EsRejectedExecutionException and the BulkRequest is dropped.
     */
    private final int readyToLoadQueueCapacity;
    private final int maxNumberOfConcurrentRequests;
    /**
     * This semaphore is used to enforce that only a certain number of BulkRequests are in flight to the server at any given time.
     */
    private final Semaphore requestsInFlightSemaphore;
    /**
     * This is the cancellable for the thread that manages the queues.
     */
    private Scheduler.Cancellable flushCancellable;
    /**
     * Once awaitClose() has been called this is set to true. At that point the flush() method begins making sure that it does not run
     * after closingTime.
     */
    private boolean isClosing = false;
    /**
     * This is the time calculated when awaitClose() is called that is the maximum System.nanoTime() when flush() is still allowed to be
     * running.
     */
    private long closingTime = -1;

    /**
     * Creates a Retry2. The returned object is not ready to be used until init() is called.
     * @param maxNumberOfRetries This is the maximum number of times a BulkRequest will be retried
     * @param scheduler A recurring task to monitor and manage this object's queues is scheduled on this scheduler
     * @param readyToLoadQueueCapacity The maximum size of the queue of BulkRequests that are ready to load to the server
     * @param maxNumberOfConcurrentRequests The maximum number of requests that can be in flight to the server from this object at any one
     *                                      time
     */
    Retry2(
        int maxNumberOfRetries,
        Scheduler scheduler,
        int readyToLoadQueueCapacity,
        int maxNumberOfConcurrentRequests,
        TimeValue queuePollingInterval
    ) {
        assert readyToLoadQueueCapacity > 0;
        assert maxNumberOfConcurrentRequests > 0;
        this.logger = LogManager.getLogger(getClass());
        this.maxNumberOfRetries = maxNumberOfRetries;
        this.scheduler = scheduler;
        this.readyToLoadQueueCapacity = readyToLoadQueueCapacity;
        this.maxNumberOfConcurrentRequests = maxNumberOfConcurrentRequests;
        this.requestsInFlightSemaphore = new Semaphore(maxNumberOfConcurrentRequests);
        /*
         * Note that the capacity for ArrayBlockingQueue is a firm capacity, and attempts to add more than that many things will get
         * rejected. But the capacity for PriorityBlockingQueue is just an initial capacity. The queue itself is unbounded. So we enforce
         *  this capacity in the code below.
         */
        this.readyToLoadQueue = new ArrayBlockingQueue<>(readyToLoadQueueCapacity);
        this.queuePollingInterval = queuePollingInterval;
    }

    /**
     * This method starts the regularly-scheduled task that monitors the queues.. It needs to be called before this class can be used.
     */
    public void init() {
        flushCancellable = scheduler.scheduleWithFixedDelay(this::flush, queuePollingInterval, ThreadPool.Names.GENERIC);
    }

    private record RetryQueuePayload(
        BulkRequest request,
        List<BulkItemResponse> responses,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        int retriesRemaining
    ) {}

    /**
     * This method queues up the given BulkRequest. If there is no room on the queue, the listener is immediately notified of failure
     * with an EsRejectedExecutionException. Otherwise, as soon as there is capacity consumer.accept(bulkRequest, actionListener) will be
     * called (from another thread). If that call fails, the BulkRequest will be queued for retry based on this class's BackoffPolicy.
     * @param consumer The consumer to which apply the request and listener. This consumer is expected to perform its work asynchronously
     *                (that is, not block the thread from which it is called).
     * @param bulkRequest The bulk request that should be executed.
     * @param listener A listener that is invoked when the bulk request finishes or completes with an exception.
     */
    public void withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest,
        ActionListener<BulkResponse> listener
    ) {
        List<BulkItemResponse> responsesAccumulator = new ArrayList<>();
        addToQueue(bulkRequest, responsesAccumulator, consumer, listener, maxNumberOfRetries);
    }

    private void addToQueue(
        BulkRequest bulkRequest,
        List<BulkItemResponse> responsesAccumulator,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        int retriesRemaining
    ) {
        boolean accepted = readyToLoadQueue.offer(
            new RetryQueuePayload(bulkRequest, responsesAccumulator, consumer, listener, retriesRemaining)
        );
        if (accepted) {
            logger.trace("Added to readyToLoadQueue. Current queue size is {} / {}", readyToLoadQueue.size(), readyToLoadQueueCapacity);
        } else {
            logger.trace("Rejecting a bulk request because the queue is full. Queue size is {}", readyToLoadQueue.size());
            listener.onFailure(
                new EsRejectedExecutionException(
                    "Could not load bulk request, bulk request queue at capacity [" + readyToLoadQueueCapacity + "]"
                )
            );
        }
    }

    /**
     * Retries the bulkRequestForRetry if the backoff Iterator has remaining backoff times.
     * @param bulkRequestForRetry The bulk request for retry. This should only include the items that have not previously succeeded
     * @param responsesAccumulator An accumulator for all BulkItemResponses for the original bulkRequest across all retries
     * @param consumer
     * @param listener The listener to be notified of success or failure on this retry or subsequent retries
     * @param retriesRemaining The number of times remaining that this BulkRequest can be retried
     */
    private void retry(
        BulkRequest bulkRequestForRetry,
        List<BulkItemResponse> responsesAccumulator,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        int retriesRemaining
    ) {
        /*
         * Here we calculate when this request will next be up for retry (in clock time) and put it on retryQueue. We use nanonTime rather
         * than currentTimeInMillis because nanoTime will not change if the system clock is updated. We do not actually care about
         * nanosecond-level resolution.
         */
        if (retriesRemaining > 0) {
            addToQueue(bulkRequestForRetry, responsesAccumulator, consumer, listener, retriesRemaining - 1);
        } else {
            listener.onFailure(
                new EsRejectedExecutionException(
                    "Could not queue bulk request for retry because the backoff policy does not allow any more retries"
                )
            );
        }
    }

    /**
     * This method servers two purposes: (1) It promotes bulk requests from the retryQueue to the readyToLoadQueue when their time to run
     * has come and (2) It calls the consumer bulk requests that are on the readyToLoadQueue.
     */
    private void flush() {
        while (isClosing == false || System.nanoTime() < closingTime) {
            boolean allowedToMakeRequest;
            long timeRemaining = isClosing ? closingTime - System.nanoTime() : 0;
            if (isClosing && timeRemaining > 0) {
                try {
                    logger.trace(
                        "Waiting up to {} for a semaphore because the server is closing",
                        new TimeValue(timeRemaining, TimeUnit.NANOSECONDS)
                    );
                    allowedToMakeRequest = requestsInFlightSemaphore.tryAcquire(timeRemaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            } else {
                allowedToMakeRequest = requestsInFlightSemaphore.tryAcquire();
            }
            if (allowedToMakeRequest == false) {
                logger.trace("Unable to acquire semaphore because {} requests are already in flight", maxNumberOfConcurrentRequests);
                /*
                 * Too many requests are already in flight, so don't flush a bulk request to Elasticsearch.
                 */
                break;
            }
            RetryQueuePayload queueItem = readyToLoadQueue.poll();
            if (queueItem == null) {
                requestsInFlightSemaphore.release();
                break;
            }
            BulkRequest bulkRequest = queueItem.request;
            logger.trace("Sending a bulk request with {} items", bulkRequest.requests.size());
            List<BulkItemResponse> responsesAccumulator = queueItem.responses;
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = queueItem.consumer;
            ActionListener<BulkResponse> listener = queueItem.listener;
            int retriesRemaining = queueItem.retriesRemaining;
            assert retriesRemaining >= 0;
            consumer.accept(
                bulkRequest,
                new RetryHandler(requestsInFlightSemaphore, bulkRequest, responsesAccumulator, consumer, listener, retriesRemaining)
            );
        }
    }

    /**
     * This method makes an attempt to run anything that is currently in either queue within the timeout given. It does not wait for
     * additional items to be put on the queues, or wait for any outstanding requests to complete.
     * @param timeout
     * @param unit
     */
    void awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        isClosing = true;
        TimeValue remainingTime = new TimeValue(timeout, unit);
        logger.trace("Starting awaitClose with timeout of {}", remainingTime);
        closingTime = System.nanoTime() + remainingTime.getNanos();
        flushCancellable.cancel();
        /*
         * The following flush will run at most until closingTime. After it completes, anything that remains is something that we didn't
         * have time to get to, or was added later.
         */
        flush();
        long timeRemaining = isClosing ? closingTime - System.nanoTime() : 0;
        requestsInFlightSemaphore.tryAcquire(maxNumberOfConcurrentRequests, timeRemaining, TimeUnit.NANOSECONDS);
        logger.trace("System time: {}, Closing time: {}", System.nanoTime(), closingTime);
        List<RetryQueuePayload> remainingReadyRequests = new ArrayList<>();
        readyToLoadQueue.drainTo(remainingReadyRequests);
        int individualRequestsRejected = 0;
        for (RetryQueuePayload request : remainingReadyRequests) {
            request.listener.onFailure(new EsRejectedExecutionException("Closing the bulk request handler"));
            individualRequestsRejected += request.request.requests.size();
        }
        logger.trace(
            "Rejecting {} requests in {} bulk requests from queue because server is closing",
            individualRequestsRejected,
            remainingReadyRequests.size()
        );
    }

    /**
     * This listener will retry any failed requests within a bulk request if possible. It only delegates to the underlying listener once
     * either all requests have succeeded or all retry attempts have been exhausted.
     */
    private final class RetryHandler implements ActionListener<BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private final Semaphore requestsInFlightSemaphore;
        private final BulkRequest bulkRequest;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final ActionListener<BulkResponse> listener;
        private final List<BulkItemResponse> responsesAccumulator;
        private final long startTimestampNanos;
        private final int retriesRemaining;

        /**
         * Creates a RetryHandler listener
         * @param requestsInFlightSemaphore This is the semaphore from which the caller has already acquired a token. The token is
         *                                  released whenever onResponse or onFailure is called on this listener.
         * @param bulkRequest The BulkRequest to be sent, a subset of the original BulkRequest.
         * @param responsesAccumulator The accumulator of all BulkItemResponses for the original BulkRequest
         * @param consumer
         * @param listener The delegate listener
         * @param retriesRemaining The number of retry attempts remaining for the bulkRequestForRetry
         */
        RetryHandler(
            Semaphore requestsInFlightSemaphore,
            BulkRequest bulkRequest,
            List<BulkItemResponse> responsesAccumulator,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            int retriesRemaining
        ) {
            this.requestsInFlightSemaphore = requestsInFlightSemaphore;
            this.bulkRequest = bulkRequest;
            this.responsesAccumulator = responsesAccumulator;
            this.consumer = consumer;
            this.listener = listener;
            this.startTimestampNanos = System.nanoTime();
            this.retriesRemaining = retriesRemaining;
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
                    retry(createBulkRequestForRetry(bulkItemResponses), responsesAccumulator, consumer, listener, retriesRemaining);
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
            boolean canRetry = ExceptionsHelper.status(e) == RETRY_STATUS && retriesRemaining > 0;
            if (canRetry) {
                retry(bulkRequest, responsesAccumulator, consumer, listener, retriesRemaining);
            } else {
                listener.onFailure(e);
            }
        }

        /**
         * This creates a new BulkRequest from only those items in the bulkItemsResponses that failed.
         * @param bulkItemResponses The latest response (including any successes and failures)
         * @return
         */
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

        /**
         * Returns true if the given bulkItemResponses can be retried.
         * @param bulkItemResponses
         * @return
         */
        private boolean canRetry(BulkResponse bulkItemResponses) {
            if (retriesRemaining == 0) {
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
