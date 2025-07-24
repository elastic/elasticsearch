/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceExecutionState;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceRequestIterator;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Implementation of bulk inference execution with throttling and concurrency control.
 * <p>
 * This runner limits the number of concurrent inference requests using a semaphore-based
 * permit system. When all permits are exhausted, additional requests are queued and
 * executed as permits become available.
 * </p>
 * <p>
 * Response processing is always executed in the ESQL worker thread pool to ensure
 * consistent thread context and avoid thread safety issues with circuit breakers
 * and other non-thread-safe components.
 * </p>
 */
public class BulkInferenceRunner {

    private final Client client;
    private final Semaphore permits;

    /**
     * Constructs a new throttled inference runner with the specified configuration.
     *
     * @param client          Client for executing inference requests
     * @param maxRunningTasks The maximum number of concurrent inference requests allowed
     */
    public BulkInferenceRunner(Client client, int maxRunningTasks) {
        this.permits = new Semaphore(maxRunningTasks);
        this.client = client;
    }

    /**
     * Executes multiple inference requests in bulk and collects all responses.
     *
     * @param requests An iterator over the inference requests to execute
     * @param listener Called with the list of all responses in request order
     */
    public void executeBulk(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener) {
        List<InferenceAction.Response> responses = new ArrayList<>();
        executeBulk(requests, responses::add, ActionListener.wrap(ignored -> listener.onResponse(responses), listener::onFailure));
    }

    /**
     * Executes multiple inference requests in bulk with streaming response handling.
     * <p>
     * This method orchestrates the entire bulk inference process:
     * 1. Creates execution state to track progress and responses
     * 2. Sets up response handling pipeline
     * 3. Initiates asynchronous request processing
     * </p>
     *
     * @param requests           An iterator over the inference requests to execute
     * @param responseConsumer   Called for each successful inference response as they complete
     * @param completionListener Called when all requests are complete or if any error occurs
     */
    public void executeBulk(
        BulkInferenceRequestIterator requests,
        Consumer<InferenceAction.Response> responseConsumer,
        ActionListener<Void> completionListener
    ) {
        if (requests.hasNext() == false) {
            completionListener.onResponse(null);
            return;
        }
        AtomicBoolean responseSent = new AtomicBoolean(false);
        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();

        final InferenceResponseListenerFactory inferenceResponseListenerFactory = (inferenceRequestItem) -> ActionListener.runAfter(
            inferenceResponseListenerFactory(bulkExecutionState, responseConsumer).create(inferenceRequestItem),
            () -> {
                if (bulkExecutionState.finished() && responseSent.compareAndSet(false, true)) {
                    if (bulkExecutionState.hasFailure() == false) {
                        try {
                            completionListener.onResponse(null);
                            return;
                        } catch (Exception e) {
                            completionListener.onFailure(e);
                        }
                    }

                    completionListener.onFailure(bulkExecutionState.getFailure());

                }
            }
        );

        executePendingRequests(requests, bulkExecutionState, inferenceResponseListenerFactory);
    }

    public ThreadPool threadPool() {
        return client.threadPool();
    }

    /**
     * Attempts to poll the next request from the iterator and acquire a permit for execution.
     * <p>
     * Because multiple threads may call this concurrently via async callbacks, this method is synchronized to ensure thread-safe access
     * to the request iterator.
     * </p>
     *
     * @param requests    The request iterator to poll from
     * @param itemFactory Factory to create BulkRequestItem instances
     * @return A BulkRequestItem if a request and permit are available, null otherwise
     */
    private synchronized BulkRequestItem pollPendingRequest(BulkInferenceRequestIterator requests, BulkRequestItem.Factory itemFactory) {
        if (requests.hasNext() && permits.tryAcquire()) {
            return itemFactory.create(requests.next());
        }

        return null;
    }

    /**
     * Main execution loop that processes inference requests asynchronously.
     * <p>
     * This method implements a continuation-based asynchronous pattern:
     * 1. Polls for the next available request (respecting concurrency limits)
     * 2. Executes the request
     * 3. Sets up a continuation callback to process the next request
     * 4. Continues until all requests are processed or an error occurs
     * </p>
     * <p>
     * The loop terminates when:
     * - No more requests are available and no permits can be acquired
     * - The bulk execution is marked as finished (due to completion or failure)
     * </p>
     *
     * @param requests                         The request iterator
     * @param bulkExecutionState               State tracker for the bulk operation
     * @param inferenceResponseListenerFactory Factory for creating response listeners
     */
    private void executePendingRequests(
        BulkInferenceRequestIterator requests,
        BulkInferenceExecutionState bulkExecutionState,
        InferenceResponseListenerFactory inferenceResponseListenerFactory
    ) {
        BulkRequestItem.Factory bulkItemFactory = new BulkRequestItem.Factory(bulkExecutionState);
        while (bulkExecutionState.finished() == false) {
            BulkRequestItem bulkRequestItem = pollPendingRequest(requests, bulkItemFactory);

            if (bulkRequestItem == null) {
                // No more requests available or at max concurrency limit
                // Release the permit we never used and stop processing
                permits.release();
                return;
            }

            if (requests.hasNext() == false) {
                // This is the last request - mark bulk execution as finished
                // to prevent further processing attempts
                bulkExecutionState.finish();
            }

            // Create response listener with continuation callback
            ActionListener<InferenceAction.Response> inferenceResponseListener = ActionListener.releaseAfter(
                inferenceResponseListenerFactory.create(bulkRequestItem),
                () -> {
                    try {
                        // Release the permit after request completion and continue processing
                        // This creates the asynchronous continuation chain
                        permits.release();
                        executePendingRequests(requests, bulkExecutionState, inferenceResponseListenerFactory);
                    } catch (Exception e) {
                        // Ensure any errors in continuation don't break the bulk operation
                        bulkExecutionState.addFailure(e);
                    }
                }
            );

            // Handle null requests (edge case in some iterators)
            if (bulkRequestItem.request() == null) {
                inferenceResponseListener.onResponse(null);
                return;
            }

            // Execute the inference request with proper origin context
            executeAsyncWithOrigin(
                client,
                INFERENCE_ORIGIN,
                InferenceAction.INSTANCE,
                bulkRequestItem.request(),
                inferenceResponseListener
            );
        }
    }

    /**
     * Creates a factory for generating response listeners for inference requests.
     * <p>
     * Each listener handles:
     * 1. Success: Records the response in the execution state
     * 2. Failure: Records the exception in the execution state
     * 3. Completion: Triggers response persistence and potential bulk completion
     * </p>
     *
     * @param bulkExecutionState The state tracker for this bulk operation
     * @param responseConsumer   Consumer to deliver processed responses to
     * @return Factory that creates properly configured response listeners
     */
    private InferenceResponseListenerFactory inferenceResponseListenerFactory(
        BulkInferenceExecutionState bulkExecutionState,
        Consumer<InferenceAction.Response> responseConsumer
    ) {
        return bulkRequestItem -> ActionListener.runAfter(
            ActionListener.wrap(
                r -> bulkExecutionState.onInferenceResponse(bulkRequestItem.seqNo(), r),
                e -> bulkExecutionState.onInferenceException(bulkRequestItem.seqNo(), e)
            ),
            () -> persistPendingResponses(bulkExecutionState, responseConsumer)
        );
    }

    /**
     * Processes and delivers buffered responses in order, ensuring proper sequencing.
     * <p>
     * This method is synchronized to ensure thread-safe access to the execution state
     * and prevent concurrent response processing which could cause ordering issues.
     * Processing stops immediately if a failure is detected to implement fail-fast behavior.
     * </p>
     *
     * @param bulkExecutionState The state tracker containing buffered responses
     * @param responseConsumer   Consumer to deliver responses to
     */
    private void persistPendingResponses(
        BulkInferenceExecutionState bulkExecutionState,
        Consumer<InferenceAction.Response> responseConsumer
    ) {
        synchronized (bulkExecutionState) {
            long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

            while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
                persistedSeqNo++;
                if (bulkExecutionState.hasFailure() == false) {
                    try {
                        InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                        synchronized (this) {
                            responseConsumer.accept(response);
                        }
                    } catch (Exception e) {
                        bulkExecutionState.addFailure(e);
                    }
                }
                bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
            }
        }
    }

    /**
     * Functional interface for creating response listeners for bulk request items.
     * <p>
     * This abstraction allows for consistent listener creation while encapsulating
     * the specific response handling logic for each request.
     * </p>
     */
    @FunctionalInterface
    private interface InferenceResponseListenerFactory {
        /**
         * Creates a response listener for the given bulk request item.
         *
         * @param bulkRequestItem The request item needing a response listener
         * @return Configured ActionListener for handling the inference response
         */
        ActionListener<InferenceAction.Response> create(BulkRequestItem bulkRequestItem);
    }

    /**
     * Encapsulates an inference request with its associated sequence number.
     * <p>
     * The sequence number is used for ordering responses and tracking completion
     * in the bulk execution state.
     * </p>
     *
     * @param seqNo   Unique sequence number for this request in the bulk operation
     * @param request The actual inference request to execute
     */
    private record BulkRequestItem(long seqNo, InferenceAction.Request request) {
        /**
         * Factory for creating BulkRequestItem instances with auto-generated sequence numbers.
         */
        private record Factory(BulkInferenceExecutionState bulkInferenceExecutionState) {
            /**
             * Creates a new BulkRequestItem with the next available sequence number.
             *
             * @param request The inference request to wrap
             * @return BulkRequestItem with assigned sequence number
             */
            public BulkRequestItem create(InferenceAction.Request request) {
                return new BulkRequestItem(bulkInferenceExecutionState.generateSeqNo(), request);
            }
        }
    }

    /**
     * Returns the executor service for ES|QL worker threads.
     *
     * @param threadPool Thread pool to use to run inference tasks
     * @return The executor service for ES|QL worker threads
     */
    private static ExecutorService executorService(ThreadPool threadPool) {
        return threadPool.executor(EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME);
    }

    public static Factory factory(Client client) {
        return inferenceRunnerConfig -> new BulkInferenceRunner(client, inferenceRunnerConfig.maxOutstandingBulkRequests());
    }

    /**
     * Factory interface for creating {@link BulkInferenceRunner} instances.
     */
    @FunctionalInterface
    public interface Factory {
        /**
         * Creates a new inference runner with the specified execution configuration.
         *
         * @param inferenceRunnerConfig Configuration defining concurrency limits and execution parameters
         * @return A configured inference runner implementation
         */
        BulkInferenceRunner create(InferenceRunnerConfig inferenceRunnerConfig);
    }
}
