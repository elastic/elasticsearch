/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    private final int maxRunningTasks;
    private final ExecutorService executor;

    /**
     * Tracks bulk requests that are currently queued to prevent duplicates.
     * <p>
     * This set ensures fairness among multiple concurrent bulk operations by preventing
     * the same bulk request from being queued multiple times. Uses ConcurrentHashMap.newKeySet()
     * for lock-free thread-safe operations.
     * </p>
     */
    private final Set<BulkInferenceRequest> trackedRequests = ConcurrentHashMap.newKeySet();

    /**
     * Queue of pending bulk requests waiting for permit availability.
     * <p>
     * Works in conjunction with {@link #trackedRequests} to ensure no duplicate requests
     * are queued while maintaining lock-free concurrent access.
     * </p>
     */
    private final Queue<BulkInferenceRequest> pendingBulkRequests = new ConcurrentLinkedQueue<>();

    /**
     * Constructs a new throttled inference runner with the specified configuration.
     *
     * @param client          Client for executing inference requests
     * @param maxRunningTasks The maximum number of concurrent inference requests allowed
     */
    public BulkInferenceRunner(Client client, int maxRunningTasks) {
        this.permits = new Semaphore(maxRunningTasks);
        this.maxRunningTasks = maxRunningTasks;
        this.client = client;
        this.executor = client.threadPool().executor(ThreadPool.Names.SEARCH);
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
        BulkInferenceRequestItemIterator requests,
        Consumer<BulkInferenceResponse> responseConsumer,
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
        private final BulkInferenceRequestItemIterator requests;
        private final Consumer<BulkInferenceResponse> responseConsumer;
        private final ActionListener<Void> completionListener;

        private final BulkInferenceExecutionState executionState;
        private final AtomicBoolean responseSent = new AtomicBoolean(false);

        BulkInferenceRequest(
            BulkInferenceRequestItemIterator requests,
            Consumer<BulkInferenceResponse> responseConsumer,
            ActionListener<Void> completionListener
        ) {
            this.requests = requests;
            this.responseConsumer = responseConsumer;
            this.completionListener = completionListener;

            // Initialize buffer capacity based on expected out-of-order responses.
            // Use the minimum of:
            // 1. Half of maxRunningTasks (typical out-of-order buffer size with good network conditions)
            // 2. Estimated request size (if smaller, cap at that)
            // This balances memory efficiency with avoiding rehashing for typical workloads.
            int estimatedSize = requests.estimatedSize();
            int bufferCapacity = Math.max(1, Math.min(estimatedSize, maxRunningTasks) / 2);
            this.executionState = new BulkInferenceExecutionState(bufferCapacity);
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
        private BulkInferenceRequestItem pollPendingRequest() {
            synchronized (requests) {
                if (requests.hasNext()) {
                    return requests.next().withSeqNo(executionState.generateSeqNo());
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
         * - Hybrid recursion strategy: Uses direct recursion for performance up to 500 levels,
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
         * 6. Uses hybrid recursion: direct calls up to 500 levels, executor-based beyond that
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
                            // Add to tracking set first to prevent duplicates
                            if (trackedRequests.add(this)) {
                                pendingBulkRequests.offer(this);
                            }
                        }
                        return;
                    } else {
                        BulkInferenceRequestItem bulkInferenceRequestItem = pollPendingRequest();

                        if (bulkInferenceRequestItem == null) {
                            // No more requests available
                            // Release the permit we didn't used and stop processing
                            permits.release();

                            // Check if another bulk request is pending for execution.
                            BulkInferenceRequest nexBulkRequest = pendingBulkRequests.poll();

                            while (nexBulkRequest == this) {
                                nexBulkRequest = pendingBulkRequests.poll();
                            }

                            if (nexBulkRequest != null) {
                                // Remove from tracking set since we're about to process it
                                trackedRequests.remove(nexBulkRequest);
                                // Execute the next bulk request with reset recursion depth
                                // Use final variable for lambda capture
                                executor.execute(nexBulkRequest::executePendingRequests);
                            }

                            return;
                        }

                        if (requests.hasNext() == false) {
                            // This is the last request - mark bulk execution as finished
                            // to prevent further processing attempts
                            executionState.finish();
                        }

                        final ActionListener<InferenceAction.Response> inferenceResponseListener = new ThreadedActionListener<>(
                            executor,
                            ActionListener.runAfter(ActionListener.wrap(r -> {
                                BulkInferenceResponse bulkResponse = new BulkInferenceResponse(bulkInferenceRequestItem, r);
                                executionState.onInferenceResponse(bulkResponse);
                            }, e -> executionState.onInferenceException(bulkInferenceRequestItem.seqNo(), e)), () -> {
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
                                            // Remove from tracking set since we're about to process it
                                            trackedRequests.remove(nexBulkRequest);
                                            // Execute the next bulk request with reset recursion depth
                                            // Use final variable for lambda capture
                                            executor.execute(nexBulkRequest::executePendingRequests);
                                        }
                                        return;
                                    }
                                    if (executionState.finished() == false) {
                                        // Execute any pending requests if any
                                        if (recursionDepth > 500) {
                                            // Reset recursion depth by submitting to executor
                                            // This prevents unbounded stack growth while maintaining performance
                                            executor.execute(this::executePendingRequests);
                                        } else {
                                            this.executePendingRequests(recursionDepth + 1);
                                        }
                                    }
                                } catch (Exception e) {
                                    if (responseSent.compareAndSet(false, true)) {
                                        // Clean up tracking set before notifying failure
                                        trackedRequests.remove(BulkInferenceRequest.this);
                                        completionListener.onFailure(e);
                                    }
                                }
                            })
                        );

                        // Handle null requests (edge case in some iterators)
                        if (bulkInferenceRequestItem.request() == null) {
                            inferenceResponseListener.onResponse(null);
                            continue;
                        }

                        // Execute the inference request with proper origin context
                        executeAsyncWithOrigin(
                            client,
                            INFERENCE_ORIGIN,
                            InferenceAction.INSTANCE,
                            bulkInferenceRequestItem.request(),
                            inferenceResponseListener
                        );
                    }
                }
            } catch (Exception e) {
                executionState.addFailure(e);
                // Ensure cleanup on exception - remove from tracking set to prevent memory leak
                trackedRequests.remove(this);
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
                        BulkInferenceResponse response = executionState.fetchBufferedResponse(persistedSeqNo);
                        if (response != null) {
                            responseConsumer.accept(response);
                        }
                    } catch (Exception e) {
                        executionState.addFailure(e);
                    }
                }
                executionState.markSeqNoAsPersisted(persistedSeqNo);
            }
        }

        /**
         * Call the completion listener when all requests have completed.
         * Also ensures cleanup of this request from tracking structures to prevent memory leaks.
         */
        private void onBulkCompletion() {
            try {
                // Clean up tracking - remove this request from the tracking set
                // in case it was queued but never processed
                trackedRequests.remove(this);

                if (executionState.hasFailure() == false) {
                    try {
                        completionListener.onResponse(null);
                        return;
                    } catch (Exception e) {
                        executionState.addFailure(e);
                    }
                }

                completionListener.onFailure(executionState.getFailure());
            } finally {
                // Ensure we're removed even if completion listener throws
                trackedRequests.remove(this);
            }
        }
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
