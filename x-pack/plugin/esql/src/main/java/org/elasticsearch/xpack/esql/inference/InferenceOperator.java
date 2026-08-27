/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

/**
 * An abstract asynchronous operator that performs throttled bulk inference execution.
 * <p>
 * The {@code InferenceOperator} integrates with the compute framework and supports throttled bulk execution of inference requests. It
 * transforms input {@link Page} into inference requests, asynchronously executes them, and uses the responses to build a new {@link Page}.
 * </p>
 */
public abstract class InferenceOperator extends AsyncOperator<InferenceOperator.OngoingInferenceResult> {

    public static final int DEFAULT_MAX_OUTSTANDING_PAGES = 10;
    public static final int DEFAULT_MAX_OUTSTANDING_REQUESTS = 50;

    private final InferenceService inferenceService;
    private final OutputBuilder outputBuilder;
    private final BulkInferenceRequestItemIterator.Factory inferenceRequestsFactory;
    private final Queue<BulkInferenceOperation> ongoingBulkOperations;
    private final Semaphore permits;

    /**
     * Collects the per-row failure warnings emitted when {@link #tolerateFailures} is {@code true}. Built up front rather
     * than on first failure because {@link #onInferenceRequestFailure} runs concurrently on search threads: a lazily created
     * collector could be built more than once, and each copy would carry its own {@code MAX_ADDED_WARNINGS} budget.
     */
    private final Warnings warnings;

    /**
     * When {@code true}, an inference request that fails does not fail the whole query: a warning is emitted, the
     * corresponding output row is filled with null, and processing continues. When {@code false} (the default for
     * COMPLETION/RERANK and the fold-based inference functions), the first failure fails the query.
     */
    private final boolean tolerateFailures;

    /**
     * Constructs a new {@code InferenceOperator}.
     *
     * @param driverContext The driver context.
     * @param inferenceService The inference service to use for executing inference requests.
     * @param source The source location for per-row failure warnings (only used when {@code tolerateFailures} is true).
     * @param tolerateFailures Whether a single failed inference request should warn, emit null, and continue instead of failing the query.
     * @param maxOutstandingPages The maximum number of pages processed in parallel.
     * @param maxOutstandingInferenceRequests The maximum number of inference requests to be run in parallel.
     */
    public InferenceOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        BulkInferenceRequestItemIterator.Factory inferenceRequestsFactory,
        OutputBuilder outputBuilder,
        WarningSourceLocation source,
        boolean tolerateFailures,
        int maxOutstandingPages,
        int maxOutstandingInferenceRequests
    ) {
        super(driverContext, inferenceService.threadContext(), maxOutstandingPages);
        this.inferenceService = inferenceService;
        this.inferenceRequestsFactory = inferenceRequestsFactory;
        this.permits = new Semaphore(maxOutstandingInferenceRequests);
        this.outputBuilder = outputBuilder;
        this.ongoingBulkOperations = ConcurrentCollections.newQueue();
        this.warnings = driverContext.createWarnings(source);
        this.tolerateFailures = tolerateFailures;
    }

    /**
     * Constructs a new {@code InferenceOperator} with default throttling parameters.
     * Use default max outstanding pages and requests settings.
     *
     * @param driverContext The driver context.
     * @param inferenceService The inference service to use for executing inference requests.
     * @param inferenceRequestsFactory Factory for creating inference request iterators from input pages.
     * @param outputBuilder Builder for converting inference responses into output pages.
     * @param source The source location for per-row failure warnings (only used when {@code tolerateFailures} is true).
     * @param tolerateFailures Whether a single failed inference request should warn, emit null, and continue instead of failing the query.
     */
    public InferenceOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        BulkInferenceRequestItemIterator.Factory inferenceRequestsFactory,
        OutputBuilder outputBuilder,
        WarningSourceLocation source,
        boolean tolerateFailures
    ) {
        this(
            driverContext,
            inferenceService,
            inferenceRequestsFactory,
            outputBuilder,
            source,
            tolerateFailures,
            DEFAULT_MAX_OUTSTANDING_PAGES,
            DEFAULT_MAX_OUTSTANDING_REQUESTS
        );
    }

    @Override
    protected void performAsync(Page input, ActionListener<OngoingInferenceResult> listener) {
        try {
            BulkInferenceRequestItemIterator requests = inferenceRequestsFactory.create(input);
            listener = ActionListener.releaseBefore(requests, listener);
            BulkInferenceOperation bulkOperation = new BulkInferenceOperation(
                requests,
                listener.safeMap(responses -> new OngoingInferenceResult(input, responses))
            );
            ongoingBulkOperations.add(bulkOperation);
            executePendingBulkOperations();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public Page getOutput() {
        OngoingInferenceResult result = fetchFromBuffer();

        if (result == null) {
            return null;
        }

        try {
            return outputBuilder.buildOutputPage(result.inputPage(), result.responses());
        } catch (Exception e) {
            releaseFetchedOnAnyThread(result);
            throw e;
        }
    }

    @Override
    protected void releaseFetchedOnAnyThread(OngoingInferenceResult ongoingInferenceResult) {
        Releasables.closeExpectNoException(ongoingInferenceResult);
    }

    @Override
    protected void doClose() {
        Releasables.closeExpectNoException(inferenceRequestsFactory);
    }

    protected String inferenceId() {
        return inferenceRequestsFactory.inferenceId();
    }

    /**
     * Schedules pending inference requests from ongoing bulk operations.
     * Processes one request from the head operation per loop iteration, then re-queues it.
     * Called after each inference completes, creating a self-perpetuating scheduling loop.
     */
    private void executePendingBulkOperations() {
        while (permits.tryAcquire()) {
            BulkInferenceOperation bulkOperation = ongoingBulkOperations.poll();
            if (bulkOperation == null) {
                // No more pending bulk operations to process
                permits.release();
                break;
            }

            BulkInferenceRequestItem requestItem = bulkOperation.pollNextRequest();

            if (requestItem == null) {
                // No more requests in this bulk operation. Complete and remove it.
                permits.release();
                bulkOperation.completeIfFinished();
                continue;
            }

            // Execute the inference request
            executeInferenceRequest(bulkOperation, requestItem);

            // Re-queue the bulk operation for further processing
            ongoingBulkOperations.add(bulkOperation);
        }
    }

    /**
     * Dispatches an inference request to the appropriate InferenceService method.
     * Subclasses may override this to route to a different service method (e.g. {@code executeEmbeddingInference}).
     */
    protected void dispatchInferenceRequest(
        InferenceService inferenceService,
        BaseInferenceActionRequest request,
        ActionListener<InferenceAction.Response> listener
    ) {
        inferenceService.executeInference((InferenceAction.Request) request, listener);
    }

    /**
     * Executes a single inference request and handles the response.
     *
     * @param bulkOperation The bulk inference operation managing the request.
     * @param request       The inference request item to execute.
     */
    private void executeInferenceRequest(BulkInferenceOperation bulkOperation, BulkInferenceRequestItem request) {
        if (request.inferenceRequest() == null) {
            bulkOperation.onInferenceResponse(request.createResponse(null));
            permits.release();
            executePendingBulkOperations();
            return;
        }

        dispatchInferenceRequest(
            inferenceService,
            request.inferenceRequest(),
            new ThreadedActionListener<>(
                inferenceService.threadPool().executor(ThreadPool.Names.SEARCH),
                ActionListener.runAfter(
                    ActionListener.wrap(
                        inferenceResponse -> bulkOperation.onInferenceResponse(request.createResponse(inferenceResponse)),
                        e -> onInferenceRequestFailure(bulkOperation, request, e)
                    ),
                    () -> {
                        permits.release();
                        executePendingBulkOperations();
                    }
                )
            )
        );
    }

    /**
     * Handles a failed inference request.
     * <p>
     * When {@link #tolerateFailures} is {@code true}, the failure is turned into a warning and the request is completed with a null
     * response, so its output row(s) become null and processing continues. Routing the failure as a null response (rather than through
     * {@link BulkInferenceOperation#onException}) preserves the bulk operation's sequencing and lets the remaining requests complete.
     * When {@code false}, the failure fails the whole bulk operation, preserving the historical fail-fast behavior.
     */
    private void onInferenceRequestFailure(BulkInferenceOperation bulkOperation, BulkInferenceRequestItem request, Exception e) {
        if (tolerateFailures && isFatal(e) == false) {
            warnings.registerException(e);
            bulkOperation.onInferenceResponse(request.createResponse(null));
        } else {
            bulkOperation.onException(e);
        }
    }

    /**
     * Whether a failure means the query as a whole is in trouble rather than that one row's inference failed. Such a failure
     * is never swallowed, even when failures are tolerated: continuing would hide a cancellation or memory pressure behind a
     * column of nulls, and would keep issuing inference requests for a query that should stop. The two types listed here are
     * the ones ES|QL already treats as fatal elsewhere, see {@code DataNodeRequestSender#trackShardLevelFailure}.
     */
    private static boolean isFatal(Exception e) {
        return ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null
            || ExceptionsHelper.unwrap(e, CircuitBreakingException.class) != null;
    }

    public interface OutputBuilder {
        Page buildOutputPage(Page inputPage, List<BulkInferenceResponseItem> responses);
    }

    /**
     * Represents a single inference request with metadata for result building.
     *
     * @param inferenceRequest   The inference request (may be null to represent a null input).
     * @param positionValueCounts Array where each element indicates how many values the corresponding input position contributed.
     *                            For example, [1, 0, 2] means position 0 contributed 1 value, position 1 was null/empty,
     *                            and position 2 contributed 2 values (multi-valued field).
     * @param seqNo The sequence number for ordering.
     */
    public record BulkInferenceRequestItem(BaseInferenceActionRequest inferenceRequest, int[] positionValueCounts, long seqNo) {

        public static final int[] SINGLE_ZERO_POSITION_VALUE_COUNTS = new int[] { 0 };
        public static final int[] SINGLE_ONE_POSITION_VALUE_COUNTS = new int[] { 1 };

        public static PositionValueCountsBuilder positionValueCountsBuilder() {
            return new PositionValueCountsBuilder(1);
        }

        public static PositionValueCountsBuilder positionValueCountsBuilder(int capacity) {
            return new PositionValueCountsBuilder(capacity);
        }

        private static final long NO_SEQ_NO = -1L;

        /**
         * Constructor for batched requests without sequence number.
         */
        public BulkInferenceRequestItem(BaseInferenceActionRequest inferenceRequest, PositionValueCountsBuilder positionValueCounts) {
            this(inferenceRequest, positionValueCounts.build(), NO_SEQ_NO);
        }

        public BulkInferenceRequestItem withSeqNo(long seqNo) {
            return new BulkInferenceRequestItem(this.inferenceRequest, this.positionValueCounts, seqNo);
        }

        public BulkInferenceResponseItem createResponse(InferenceAction.Response inferenceResponse) {
            return new BulkInferenceResponseItem(inferenceResponse, this.positionValueCounts, this.seqNo);
        }

        /**
         * Builder for constructing position value counts arrays dynamically.
         * Each element in the array represents how many values a specific input position contributed.
         */
        public static class PositionValueCountsBuilder {
            private int[] buffer;
            private int size = 0;

            PositionValueCountsBuilder(int initialCapacity) {
                this.buffer = new int[initialCapacity];
            }

            /**
             * Resets the builder to an empty state.
             */
            public void reset() {
                size = 0;
            }

            /**
             * Adds a value count for the next position, expanding the buffer if necessary.
             */
            public void addValue(int value) {
                if (size >= buffer.length) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                }

                buffer[size++] = value;
            }

            /**
             * Builds the final position value counts array, optimizing for common cases.
             */
            public int[] build() {
                assert size > 0 : "Position value counts must have at least one entry";

                // Optimize common single-element cases
                if (size == 1) {
                    if (buffer[0] == 0) {
                        return SINGLE_ZERO_POSITION_VALUE_COUNTS;
                    } else if (buffer[0] == 1) {
                        return SINGLE_ONE_POSITION_VALUE_COUNTS;
                    }
                }

                return Arrays.copyOf(buffer, size);
            }
        }
    }

    public interface BulkInferenceRequestItemIterator extends Iterator<BulkInferenceRequestItem>, Releasable {
        /**
         * Estimates the number of requests provided by this iterator.
         */
        int estimatedSize();

        /**
         * Factory interface for creating {@link BulkInferenceOperation} instances from input pages.
         */
        interface Factory extends Releasable {
            BulkInferenceRequestItemIterator create(Page inputPage);

            String inferenceId();
        }
    }

    /**
     * Represents a completed inference response with metadata for result building.
     *
     * @param inferenceResponse   The inference response (may be null for null requests).
     * @param positionValueCounts Array where each element indicates how many values the corresponding input position contributed.
     * @param seqNo               The sequence number for ordering.
     */
    public record BulkInferenceResponseItem(InferenceAction.Response inferenceResponse, int[] positionValueCounts, long seqNo) {}

    /**
     * Manages the execution of inference requests for a single input page.
     */
    public static class BulkInferenceOperation {

        private final BulkInferenceRequestItemIterator requestItemIterator;
        private final ActionListener<List<BulkInferenceResponseItem>> completionListener;

        private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);

        private final List<BulkInferenceResponseItem> responses;
        private final Map<Long, BulkInferenceResponseItem> bufferedResponses;
        private final SetOnce<Exception> exception = new SetOnce<>();
        private final AtomicBoolean completed = new AtomicBoolean(false);

        public BulkInferenceOperation(
            BulkInferenceRequestItemIterator requestItemIterator,
            ActionListener<List<BulkInferenceResponseItem>> completionListener
        ) {
            this.requestItemIterator = requestItemIterator;
            this.completionListener = completionListener;
            this.responses = new ArrayList<>(requestItemIterator.estimatedSize());
            this.bufferedResponses = new HashMap<>(DEFAULT_MAX_OUTSTANDING_REQUESTS);
        }

        /**
         * Polls the next inference request, assigning it a sequence number for ordering.
         *
         * @return The next request item with sequence number, or null if no more requests or operation is finished.
         */
        public BulkInferenceRequestItem pollNextRequest() {
            if (hasFailure() || completed.get()) {
                return null;
            }

            synchronized (checkpoint) {
                // Re-check under the lock: completeIfFinished() releases the request iterator (and its backing block) while holding
                // this lock when a failure occurs. Without re-checking, a concurrent poller that passed the guard above could invoke
                // requestItemIterator.next() on an already-released block.
                if (hasFailure() || completed.get() || requestItemIterator.hasNext() == false) {
                    return null;
                }

                return requestItemIterator.next().withSeqNo(checkpoint.generateSeqNo());
            }
        }

        /**
         * Handles an inference response, buffering it and draining responses in sequence order.
         * Responses are reordered using the checkpoint tracker to maintain correct sequencing.
         *
         * @param response The inference response to process.
         */
        public void onInferenceResponse(BulkInferenceResponseItem response) {
            if (hasFailure() || isCompleted()) {
                return;
            }
            synchronized (checkpoint) {
                bufferedResponses.put(response.seqNo(), response);
                checkpoint.markSeqNoAsProcessed(response.seqNo());
            }

            persistPendingResponses();
        }

        /**
         * Handles an exception, failing the entire bulk operation.
         * Only the first exception is recorded; subsequent exceptions are ignored.
         *
         * @param exception The exception that occurred.
         */
        public void onException(Exception exception) {
            if (this.exception.trySet(exception)) {
                completeIfFinished();
            }
        }

        /**
         * Completes the operation if all requests have been sent and all responses have been received.
         * Calls the completion listener exactly once when the operation finishes successfully.
         */
        public void completeIfFinished() {
            synchronized (checkpoint) {
                if (completed.get()) {
                    return;
                }
                if (hasFailure() && completed.compareAndSet(false, true)) {
                    // An exception occurred during execution.
                    // Fail the operation.
                    completionListener.onFailure(exception.get());
                    clearBuffers();
                    return;
                }

                if (allRequestsSent() && allRequestsProcessed() && completed.compareAndSet(false, true)) {
                    completionListener.onResponse(Collections.unmodifiableList(responses));
                    clearBuffers();
                }
            }
        }

        public void clearBuffers() {
            bufferedResponses.clear();
            if (hasFailure()) {
                responses.clear();
            }
        }

        private void persistPendingResponses() {
            synchronized (checkpoint) {
                long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
                while (persistedCheckpoint++ < checkpoint.getProcessedCheckpoint()) {
                    BulkInferenceResponseItem response = bufferedResponses.remove(persistedCheckpoint);
                    if (response == null) {
                        throw new IllegalStateException("Missing buffered response for seqNo " + persistedCheckpoint);
                    }
                    responses.add(response);
                    checkpoint.markSeqNoAsPersisted(response.seqNo());
                }
            }
            completeIfFinished();
        }

        private boolean isCompleted() {
            return completed.get();
        }

        private boolean allRequestsSent() {
            return requestItemIterator.hasNext() == false;
        }

        private boolean allRequestsProcessed() {
            return checkpoint.getPersistedCheckpoint() == checkpoint.getMaxSeqNo();
        }

        private boolean hasFailure() {
            return exception.get() != null;
        }
    }

    /**
     * Represents the result of a bulk inference operation for a single input page.
     *
     * @param inputPage The original input page.
     * @param responses The list of inference responses corresponding to the input requests.
     */
    public record OngoingInferenceResult(Page inputPage, List<BulkInferenceResponseItem> responses) implements Releasable {
        @Override
        public void close() {
            releasePageOnAnyThread(inputPage);
        }
    }
}
