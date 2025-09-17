/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.esql.plugin.EsqlPlugin.ESQL_WORKER_THREAD_POOL_NAME;

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
    private final ExecutorService executor;

    /**
     * Custom concurrent queue that prevents duplicate bulk requests from being queued.
     * <p>
     * This queue implementation ensures fairness among multiple concurrent bulk operations
     * by preventing the same bulk request from being queued multiple times. It uses a
     * backing concurrent set to track which requests are already queued.
     * </p>
     */
    private final Queue<BulkInferenceRequest> pendingBulkRequests = new ConcurrentLinkedQueue<>() {
        private final Set<BulkInferenceRequest> requests = ConcurrentCollections.newConcurrentSet();

        @Override
        public boolean offer(BulkInferenceRequest bulkInferenceRequest) {
            synchronized (requests) {
                if (requests.add(bulkInferenceRequest)) {
                    return super.offer(bulkInferenceRequest);
                }
                return false; // Already exists, don't add duplicate
            }
        }

        @Override
        public BulkInferenceRequest poll() {
            synchronized (requests) {
                BulkInferenceRequest request = super.poll();
                if (request != null) {
                    requests.remove(request);
                }
                return request;
            }
        }
    };

    /**
     * Constructs a new throttled inference runner with the specified configuration.
     *
     * @param client          Client for executing inference requests
     * @param maxRunningTasks The maximum number of concurrent inference requests allowed
     */
    public BulkInferenceRunner(Client client, int maxRunningTasks) {
        this.permits = new Semaphore(maxRunningTasks);
        this.client = client;
        this.executor = client.threadPool().executor(ESQL_WORKER_THREAD_POOL_NAME);
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

        new BulkInferenceRequest(requests, responseConsumer, completionListener).executePendingRequests();
    }

    /**
     * Returns the thread pool used for executing inference requests.
     */
    public ThreadPool threadPool() {
        return client.threadPool();
    }

    /**
     * Encapsulates the execution state and logic for a single bulk inference operation.
     * <p>
     * This inner class manages the complete lifecycle of a bulk inference request, including:
     * - Request iteration and permit-based concurrency control
     * - Asynchronous execution with hybrid recursion strategy
     * - Response collection and ordering via execution state
     * - Error handling and completion notification
     * </p>
     * <p>
     * Each BulkInferenceRequest instance represents one bulk operation that may contain
     * multiple individual inference requests. Multiple BulkInferenceRequest instances
     * can execute concurrently, with fairness ensured through the pending queue mechanism.
     * </p>
     */
    private class BulkInferenceRequest {
        private final BulkInferenceRequestIterator requests;
        private final Consumer<InferenceAction.Response> responseConsumer;
        private final ActionListener<Void> completionListener;

        private final BulkInferenceExecutionState executionState = new BulkInferenceExecutionState();
        private final AtomicBoolean responseSent = new AtomicBoolean(false);

        BulkInferenceRequest(
            BulkInferenceRequestIterator requests,
            Consumer<InferenceAction.Response> responseConsumer,
            ActionListener<Void> completionListener
        ) {
            this.requests = requests;
            this.responseConsumer = responseConsumer;
            this.completionListener = completionListener;
        }

        /**
         * Attempts to poll the next request from the iterator and acquire a permit for execution.
         * <p>
         * Because multiple threads may call this concurrently via async callbacks, this method is synchronized to ensure thread-safe access
         * to the request iterator.
         * </p>
         *
         * @return A BulkRequestItem if a request and permit are available, null otherwise
         */
        private BulkRequestItem pollPendingRequest() {
            synchronized (requests) {
                if (requests.hasNext()) {
                    return new BulkRequestItem(executionState.generateSeqNo(), requests.next());
                }
            }

            return null;
        }

        /**
         * Main execution loop that processes inference requests asynchronously with hybrid recursion strategy.
         * <p>
         * This method implements a continuation-based asynchronous pattern with the following features:
         * - Queue-based fairness: Multiple bulk requests can be queued and processed fairly
         * - Permit-based concurrency control: Limits concurrent inference requests using semaphores
         * - Hybrid recursion strategy: Uses direct recursion for performance up to 100 levels,
         * then switches to executor-based continuation to prevent stack overflow
         * - Duplicate prevention: Custom queue prevents the same bulk request from being queued multiple times
         * </p>
         * <p>
         * Execution flow:
         * 1. Attempts to acquire a permit for concurrent execution
         * 2. If no permit available, queues this bulk request for later execution
         * 3. Polls for the next available request from the iterator
         * 4. If no requests available, schedules the next queued bulk request
         * 5. Executes the request asynchronously with proper continuation handling
         * 6. Uses hybrid recursion: direct calls up to 100 levels, executor-based beyond that
         * </p>
         * <p>
         * The loop terminates when:
         * - No more requests are available and no permits can be acquired
         * - The bulk execution is marked as finished (due to completion or failure)
         * - An unrecoverable error occurs during processing
         * </p>
         */
        private void executePendingRequests() {
            executePendingRequests(0);
        }

        private void executePendingRequests(int recursionDepth) {
            try {
                while (executionState.finished() == false) {
                    if (permits.tryAcquire() == false) {
                        if (requests.hasNext()) {
                            pendingBulkRequests.add(this);
                        }
                        return;
                    } else {
                        BulkRequestItem bulkRequestItem = pollPendingRequest();

                        if (bulkRequestItem == null) {
                            // No more requests available
                            // Release the permit we didn't used and stop processing
                            permits.release();

                            // Check if another bulk request is pending for execution.
                            BulkInferenceRequest nexBulkRequest = pendingBulkRequests.poll();

                            while (nexBulkRequest == this) {
                                nexBulkRequest = pendingBulkRequests.poll();
                            }

                            if (nexBulkRequest != null) {
                                executor.execute(nexBulkRequest::executePendingRequests);
                            }

                            return;
                        }

                        if (requests.hasNext() == false) {
                            // This is the last request - mark bulk execution as finished
                            // to prevent further processing attempts
                            executionState.finish();
                        }

                        final ActionListener<InferenceAction.Response> inferenceResponseListener = ActionListener.runAfter(
                            ActionListener.wrap(
                                r -> executionState.onInferenceResponse(bulkRequestItem.seqNo(), r),
                                e -> executionState.onInferenceException(bulkRequestItem.seqNo(), e)
                            ),
                            () -> {
                                // Release the permit we used
                                permits.release();

                                try {
                                    synchronized (executionState) {
                                        persistPendingResponses();
                                    }

                                    if (executionState.finished() && responseSent.compareAndSet(false, true)) {
                                        onBulkCompletion();
                                    }

                                    if (responseSent.get()) {
                                        // Response has already been sent
                                        // No need to continue processing this bulk.
                                        // Check if another bulk request is pending for execution.
                                        BulkInferenceRequest nexBulkRequest = pendingBulkRequests.poll();
                                        if (nexBulkRequest != null) {
                                            executor.execute(nexBulkRequest::executePendingRequests);
                                        }
                                        return;
                                    }
                                    if (executionState.finished() == false) {
                                        // Execute any pending requests if any
                                        if (recursionDepth > 100) {
                                            executor.execute(this::executePendingRequests);
                                        } else {
                                            this.executePendingRequests(recursionDepth + 1);
                                        }
                                    }
                                } catch (Exception e) {
                                    if (responseSent.compareAndSet(false, true)) {
                                        completionListener.onFailure(e);
                                    }
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
            } catch (Exception e) {
                executionState.addFailure(e);
            }
        }

        /**
         * Processes and delivers buffered responses in order, ensuring proper sequencing.
         * <p>
         * This method is synchronized to ensure thread-safe access to the execution state
         * and prevent concurrent response processing which could cause ordering issues.
         * Processing stops immediately if a failure is detected to implement fail-fast behavior.
         * </p>
         */
        private void persistPendingResponses() {
            long persistedSeqNo = executionState.getPersistedCheckpoint();

            while (persistedSeqNo < executionState.getProcessedCheckpoint()) {
                persistedSeqNo++;
                if (executionState.hasFailure() == false) {
                    try {
                        InferenceAction.Response response = executionState.fetchBufferedResponse(persistedSeqNo);
                        responseConsumer.accept(response);
                    } catch (Exception e) {
                        executionState.addFailure(e);
                    }
                }
                executionState.markSeqNoAsPersisted(persistedSeqNo);
            }
        }

        /**
         * Call the completion listener when all requests have completed.
         */
        private void onBulkCompletion() {
            if (executionState.hasFailure() == false) {
                try {
                    completionListener.onResponse(null);
                    return;
                } catch (Exception e) {
                    executionState.addFailure(e);
                }
            }

            completionListener.onFailure(executionState.getFailure());
        }
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
         * @param bulkInferenceRunnerConfig Configuration defining concurrency limits and execution parameters
         * @return A configured inference runner implementation
         */
        BulkInferenceRunner create(BulkInferenceRunnerConfig bulkInferenceRunnerConfig);
    }
}
