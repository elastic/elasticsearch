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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
     * Once awaitClose() has been called this is set to true. Any new requests that come in (whether via withBackoff() or a retry) will
     * be rejected by sending EsRejectedExecutionExceptions to their listeners.
     */
    private boolean isClosing = false;

    /*
     * This is an approximation of the total number of bytes in the bulk requests that are with consumers.
     */
    private final AtomicLong totalBytesInFlight = new AtomicLong(0);
    /*
     * This is the approximate maximum number of bytes that this object will allow to be in flight to consumers.
     */
    private final long maxBytesInFlight;
    /*
     * We register in-flight calls with this Phaser so that we know whether there are any still in flight when we call awaitClose().
     */
    private final Phaser inFlightRequestsPhaser = new Phaser(1);

    /**
     * Creates a Retry2.
     * @param maxNumberOfRetries This is the maximum number of times a BulkRequest will be retried
     * @param maxBytesInFlight This is the maximum number of bytes that we want this object to keep in flight to the consumers
     */
    Retry2(int maxNumberOfRetries, ByteSizeValue maxBytesInFlight) {
        this.logger = LogManager.getLogger(getClass());
        this.maxNumberOfRetries = maxNumberOfRetries;
        this.maxBytesInFlight = maxBytesInFlight.getBytes();
    }

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
        if (isClosing) {
            listener.onFailure(new EsRejectedExecutionException("The bulk processor is closing"));
            return;
        }
        List<BulkItemResponse> responsesAccumulator = new ArrayList<>();
        logger.trace("Sending a bulk request with {} bytes in {} items", bulkRequest.estimatedSizeInBytes(), bulkRequest.requests.size());
        /*
         * Here we have a risk that multiple threads will do this check at once, and each will think it has room to run. So we could end
         * up with slightly more bytes in flight than totalBytesInFlight. But this is acceptable risk, and preferable to using a lock or
         * a while loop with compareAndSet.
         */
        long bytesInFlight = totalBytesInFlight.get();
        if (bytesInFlight + bulkRequest.estimatedSizeInBytes() > maxBytesInFlight) {
            listener.onFailure(
                new EsRejectedExecutionException(
                    "Cannot index request of size "
                        + bulkRequest.estimatedSizeInBytes()
                        + " because "
                        + totalBytesInFlight.get()
                        + " bytes are already in flight and the max is "
                        + maxBytesInFlight
                )
            );
        } else {
            totalBytesInFlight.addAndGet(bulkRequest.estimatedSizeInBytes());
            inFlightRequestsPhaser.register();
            consumer.accept(bulkRequest, new RetryHandler(bulkRequest, responsesAccumulator, consumer, listener, maxNumberOfRetries));
        }
    }

    /**
     * Retries the bulkRequestForRetry if retriesRemaining is greater than 0, otherwise notifies the listener of failure
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
        if (isClosing) {
            listener.onFailure(new EsRejectedExecutionException("The bulk processor is closing"));
            return;
        }
        if (retriesRemaining > 0) {
            inFlightRequestsPhaser.register();
            consumer.accept(
                bulkRequestForRetry,
                new RetryHandler(bulkRequestForRetry, responsesAccumulator, consumer, listener, retriesRemaining - 1)
            );
        } else {
            totalBytesInFlight.addAndGet(-1 * bulkRequestForRetry.estimatedSizeInBytes());
            listener.onFailure(
                new EsRejectedExecutionException(
                    "Could not queue bulk request for retry because the backoff policy does not allow any more retries"
                )
            );
        }
    }

    /**
     * This method makes an attempt to wait for any outstanding requests to complete. Any new requests that come in after this method has
     * been called (whether via withBackoff() or a retry) will be rejected by sending EsRejectedExecutionExceptions to their listeners.
     * @param timeout
     * @param unit
     */
    void awaitClose(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        isClosing = true;
        inFlightRequestsPhaser.arriveAndDeregister();
        inFlightRequestsPhaser.awaitAdvanceInterruptibly(0, timeout, unit);
    }

    /**
     * This listener will retry any failed requests within a bulk request if possible. It only delegates to the underlying listener once
     * either all requests have succeeded or all retry attempts have been exhausted.
     */
    private final class RetryHandler implements ActionListener<BulkResponse> {
        private static final RestStatus RETRY_STATUS = RestStatus.TOO_MANY_REQUESTS;
        private final BulkRequest bulkRequest;
        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final ActionListener<BulkResponse> listener;
        private final List<BulkItemResponse> responsesAccumulator;
        private final long startTimestampNanos;
        private final int retriesRemaining;

        /**
         * Creates a RetryHandler listener
         * @param bulkRequest The BulkRequest to be sent, a subset of the original BulkRequest.
         * @param responsesAccumulator The accumulator of all BulkItemResponses for the original BulkRequest
         * @param consumer
         * @param listener The delegate listener
         * @param retriesRemaining The number of retry attempts remaining for the bulkRequestForRetry
         */
        RetryHandler(
            BulkRequest bulkRequest,
            List<BulkItemResponse> responsesAccumulator,
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            ActionListener<BulkResponse> listener,
            int retriesRemaining
        ) {
            this.bulkRequest = bulkRequest;
            this.responsesAccumulator = responsesAccumulator;
            this.consumer = consumer;
            this.listener = listener;
            this.startTimestampNanos = System.nanoTime();
            this.retriesRemaining = retriesRemaining;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (bulkItemResponses.hasFailures() == false) {
                totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
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
                    totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
                    logger.trace(
                        "Got a response in {} with {} items including failures, cannot retry",
                        bulkItemResponses.getTook(),
                        bulkItemResponses.getItems().length
                    );
                    addResponses(bulkItemResponses, (r -> true));
                    listener.onResponse(getAccumulatedResponse());
                }
            }
            inFlightRequestsPhaser.arriveAndDeregister();
        }

        @Override
        public void onFailure(Exception e) {
            totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
            boolean canRetry = ExceptionsHelper.status(e) == RETRY_STATUS && retriesRemaining > 0;
            if (canRetry) {
                inFlightRequestsPhaser.arriveAndDeregister();
                retry(bulkRequest, responsesAccumulator, consumer, listener, retriesRemaining);
            } else {
                listener.onFailure(e);
                inFlightRequestsPhaser.arriveAndDeregister();
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
