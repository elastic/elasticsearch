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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * Encapsulates asynchronous retry logic. This class will attempt to load a BulkRequest up to numberOfRetries times. If that number of
 * times is exhausted, it sends the listener an EsRejectedExecutionException.
 */
class Retry2 {
    private static final Logger logger = LogManager.getLogger(Retry2.class);
    private final int maxNumberOfRetries;
    /**
     * Once awaitClose() has been called this is set to true. Any new requests that come in (whether via consumeRequestWithRetries() or a
     * retry) will be rejected by sending EsRejectedExecutionExceptions to their listeners.
     */
    private boolean isClosing = false;
    /*
     * We register in-flight calls with this Phaser so that we know whether there are any still in flight when we call awaitClose(). The
     * phaser is initialized with 1 party intentionally. This is because if the number of parties goes over 0 and then back down to 0 the
     * phaser is automatically terminated. Since we're tracking the number of in flight calls to Elasticsearch we expect this to happen
     * often. Putting an initial party in here makes sure that the phaser is never terminated before we're ready for it.
     */
    private final Phaser inFlightRequestsPhaser = new Phaser(1);

    /**
     * Creates a Retry2.
     * @param maxNumberOfRetries This is the maximum number of times a BulkRequest will be retried
     */
    Retry2(int maxNumberOfRetries) {
        this.maxNumberOfRetries = maxNumberOfRetries;
    }

    /**
     * This method attempts to load the given BulkRequest (via the given BiConsumer). If the initial load fails with a retry-able reason,
     * this class will retry the load up to maxNumberOfRetries times. The given ActionListener will be notified of the result, either on
     * success or after failure when no retries are left. The listener is not notified of failures if it is still possible to retry.
     * @param consumer The consumer to which apply the request and listener. This consumer is expected to perform its work asynchronously
     *                (that is, not block the thread from which it is called).
     * @param bulkRequest The bulk request that should be executed.
     * @param listener A listener that is invoked when the bulk request finishes or completes with an exception.
     */
    public void consumeRequestWithRetries(
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
        inFlightRequestsPhaser.register();
        consumer.accept(bulkRequest, new RetryHandler(bulkRequest, responsesAccumulator, consumer, listener, maxNumberOfRetries));
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
            listener.onFailure(
                new EsRejectedExecutionException(
                    "Could not retry the bulk request because the backoff policy does not allow any more retries"
                )
            );
        }
    }

    /**
     * This method makes an attempt to wait for any outstanding requests to complete. Any new requests that come in after this method has
     * been called (whether via consumeRequestWithRetries() or a retry) will be rejected by sending EsRejectedExecutionExceptions to their
     * listeners.
     * @param timeout
     * @param unit
     * @return True if all outstanding requests complete in the given time, false otherwise
     */
    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        isClosing = true;
        /*
         * This removes the party that was placed in the phaser at initialization so that the phaser will terminate once all in-flight
         * requests have been completed (i.e. this makes it possible that the number of parties can become 0).
         */
        inFlightRequestsPhaser.arriveAndDeregister();
        try {
            inFlightRequestsPhaser.awaitAdvanceInterruptibly(0, timeout, unit);
            return true;
        } catch (TimeoutException e) {
            logger.debug("Timed out waiting for all requests to complete during awaitClose");
            return false;
        }
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
         * @param responsesAccumulator The accumulator of all BulkItemResponses for the original BulkRequest. These are completed
         *                             responses, meaning responses for successes, or responses for failures only if no more retries are
         *                             allowed.
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
                    BulkRequest retryRequest = createBulkRequestForRetry(bulkItemResponses);
                    retry(retryRequest, responsesAccumulator, consumer, listener, retriesRemaining);
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
            inFlightRequestsPhaser.arriveAndDeregister();
        }

        @Override
        public void onFailure(Exception e) {
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
