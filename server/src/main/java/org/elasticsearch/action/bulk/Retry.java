/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.bulk;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Encapsulates synchronous and asynchronous retry logic.
 */
public class Retry {
    private final BackoffPolicy backoffPolicy;
    private final Scheduler scheduler;
    private final BlockingQueue<RetryQueuePayload> queue;
    private final Map<BulkRequest, Scheduler.Cancellable> bulkRequestCancellableMap = new HashMap<>();
    private final int queueCapacity;
    private final Semaphore requestsInFlightSemaphore;
    private final int maxNumberOfConcurrentRequests;
    private Scheduler.Cancellable flushCancellable;

    public Retry(BackoffPolicy backoffPolicy, Scheduler scheduler) {
        this(backoffPolicy, scheduler, 1000, 10);
    }

    public Retry(BackoffPolicy backoffPolicy, Scheduler scheduler, int queueCapacity, int maxNumberOfConcurrentRequests) {
        this.backoffPolicy = backoffPolicy;
        this.scheduler = scheduler;
        this.queueCapacity = queueCapacity;
        this.maxNumberOfConcurrentRequests = Math.max(maxNumberOfConcurrentRequests, 1);
        requestsInFlightSemaphore = new Semaphore(this.maxNumberOfConcurrentRequests);
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
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
        boolean accepted = queue.offer(new RetryQueuePayload(bulkRequest, new ArrayList<>(), consumer, listener, backoff, false));
        if (accepted == false) {
            onFailure(
                bulkRequest,
                new ArrayList<>(),
                consumer,
                listener,
                new EsRejectedExecutionException(
                    "Could not retry bulk request, bulk request queue at capacity [" + queue.size() + "/" + queueCapacity + "]"
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
            try {
                listener.onFailure(e);
            } finally {
                if (bulkRequestCancellableMap.get(bulkRequest) != null) {
                    bulkRequestCancellableMap.get(bulkRequest).cancel();
                    bulkRequestCancellableMap.remove(bulkRequest);
                }
            }
        }
    }

    private void retry(
        BulkRequest bulkRequestForRetry,
        List<BulkItemResponse> responsesAccumulator,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        ActionListener<BulkResponse> listener,
        Iterator<TimeValue> backoff
    ) {
        boolean accepted = queue.offer(new RetryQueuePayload(bulkRequestForRetry, responsesAccumulator, consumer, listener, backoff, true));
        if (accepted == false) {
            onFailure(
                bulkRequestForRetry,
                responsesAccumulator,
                consumer,
                listener,
                new EsRejectedExecutionException(
                    "Could not retry bulk request, bulk request queue at capacity [" + queue.size() + "/" + queueCapacity + "]"
                ),
                false,
                backoff
            );
        }
    }

    /**
     * Invokes #accept(BulkRequest, ActionListener). Backs off on the provided exception. Retries will be scheduled using
     * the class's thread pool.
     *
     * @param consumer The consumer to which apply the request and listener
     * @param bulkRequest The bulk request that should be executed.
     * @return a future representing the bulk response returned by the client.
     */
    public ActionFuture<BulkResponse> withBackoff(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BulkRequest bulkRequest
    ) {
        PlainActionFuture<BulkResponse> future = PlainActionFuture.newFuture();
        withBackoff(consumer, bulkRequest, future);
        return future;
    }

    void flush() {
        while (true) {
            boolean allowedToMakeRequest = requestsInFlightSemaphore.tryAcquire();
            if (allowedToMakeRequest == false) {
                /*
                 * Too many requests are already in flight, so don't flush.
                 */
                return;
            }
            RetryQueuePayload queueItem = queue.poll();
            if (queueItem == null) {
                requestsInFlightSemaphore.release();
                /*
                 * It is possible that something was added to the queue after the poll and before the semaphore was released, meaning
                 * that the other thread could not acquire the permit, leaving an item orphaned in the queue. So we check the queue
                 * again after releasing the semaphore, and if there is something there we run another loop to pick that thing up. If
                 * another thread has picked it up in the meantime, we'll just exit out of the loop on the next try.
                 */
                if (queue.isEmpty()) {
                    return;
                } else {
                    continue;
                }
            }
            BulkRequest bulkRequest = queueItem.request;
            List<BulkItemResponse> responsesAccumulator = queueItem.responses;
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer = queueItem.consumer;
            ActionListener<BulkResponse> listener = queueItem.listener;
            Iterator<TimeValue> backoff = queueItem.backoff;
            boolean isRetry = queueItem.isRetry;
            bulkRequestCancellableMap.put(
                bulkRequest,
                scheduler.schedule(
                    () -> {
                        consumer.accept(bulkRequest, new RetryHandler(bulkRequest, responsesAccumulator, consumer, listener, backoff));},
                    isRetry ? backoff.next() : TimeValue.ZERO,
                    ThreadPool.Names.SAME
                )
            );
        }
    }

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        List<RetryQueuePayload> remainingRequests = new ArrayList<>();
        queue.drainTo(remainingRequests);
        for (RetryQueuePayload request : remainingRequests) {
            request.listener.onFailure(new EsRejectedExecutionException("Closing the bulk request handler"));
        }
        boolean noRequestsInFlight = requestsInFlightSemaphore.tryAcquire(maxNumberOfConcurrentRequests, timeout, unit);
        flushCancellable.cancel();
        return noRequestsInFlight && queue.isEmpty();
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
            Retry.this.onFailure(this.bulkRequest, responsesAccumulator, consumer, delegate, e, retry, backoff);
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
            try {
                delegate.onResponse(getAccumulatedResponse());
            } finally {
                if (bulkRequestCancellableMap.get(bulkRequest) != null) {
                    bulkRequestCancellableMap.get(bulkRequest).cancel();
                    bulkRequestCancellableMap.remove(bulkRequest);
                }
            }
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
