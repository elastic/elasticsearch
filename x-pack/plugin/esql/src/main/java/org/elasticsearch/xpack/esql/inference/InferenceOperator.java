/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AsyncOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
     * Constructs a new {@code InferenceOperator}.
     *
     * @param driverContext The driver context.
     * @param inferenceService The inference service to use for executing inference requests.
     * @param maxOutstandingPages The maximum number of pages processed in parallel.
     * @param maxOutstandingInferenceRequests The maximum number of inference requests to be run in parallel.
     */
    public InferenceOperator(
        DriverContext driverContext,
        InferenceService inferenceService,
        BulkInferenceRequestItemIterator.Factory inferenceRequestsFactory,
        OutputBuilder outputBuilder,
        int maxOutstandingPages,
        int maxOutstandingInferenceRequests
    ) {
        super(driverContext, inferenceService.threadContext(), maxOutstandingPages);
        this.inferenceService = inferenceService;
        this.inferenceRequestsFactory = inferenceRequestsFactory;
        this.permits = new Semaphore(maxOutstandingInferenceRequests);
        this.outputBuilder = outputBuilder;
        this.ongoingBulkOperations = ConcurrentCollections.newQueue();
    }

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

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%s[]", this.getClass().getSimpleName());
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

        inferenceService.executeInference(
            request.inferenceRequest(),
            new ThreadedActionListener<>(
                inferenceService.threadPool().executor(ThreadPool.Names.SEARCH),
                ActionListener.runAfter(
                    ActionListener.wrap(
                        inferenceResponse -> bulkOperation.onInferenceResponse(request.createResponse(inferenceResponse)),
                        bulkOperation::onException
                    ),
                    () -> {
                        permits.release();
                        executePendingBulkOperations();
                    }
                )
            )
        );
    }

    public interface OutputBuilder {
        Page buildOutputPage(Page inputPage, List<BulkInferenceResponseItem> responses);
    }

    /**
     * Represents a single inference request with metadata for result building.
     *
     * @param inferenceRequest The inference request (may be null to represent a null input).
     * @param shape The shape array for the input that generated this request.
     * @param seqNo The sequence number for ordering.
     */
    public record BulkInferenceRequestItem(InferenceAction.Request inferenceRequest, int[] shape, long seqNo) {

        public static final int[] SINGLE_ZERO_SHAPE = new int[] { 0 };
        public static final int[] SINGLE_ONE_SHAPE = new int[] { 1 };

        public static ShapeBuilder shapeBuilder() {
            return new ShapeBuilder(1);
        }

        public static ShapeBuilder shapeBuilder(int capacity) {
            return new ShapeBuilder(capacity);
        }

        private static final long NO_SEQ_NO = -1L;

        /**
         * Constructor for batched requests without sequence number.
         */
        public BulkInferenceRequestItem(InferenceAction.Request inferenceRequest, ShapeBuilder shape) {
            this(inferenceRequest, shape.build(), NO_SEQ_NO);
        }

        public BulkInferenceRequestItem withSeqNo(long seqNo) {
            return new BulkInferenceRequestItem(this.inferenceRequest, this.shape, seqNo);
        }

        public BulkInferenceResponseItem createResponse(InferenceAction.Response inferenceResponse) {
            return new BulkInferenceResponseItem(inferenceResponse, this.shape, this.seqNo);
        }

        /**
         * Builder for constructing shape arrays dynamically.
         */
        public static class ShapeBuilder {
            private int[] buffer;
            private int size = 0;

            ShapeBuilder(int initialCapacity) {
                this.buffer = new int[initialCapacity];
            }

            /**
             * Resets the shape builder to an empty state.
             */
            public void reset() {
                size = 0;
            }

            /**
             * Adds a value to the shape array, expanding it if necessary.
             */
            public void addValue(int value) {
                if (size >= buffer.length) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                }

                buffer[size++] = value;
            }

            /**
             * Builds the final shape array, optimizing for common cases.
             */
            public int[] build() {
                assert size > 0 : "Shape must have at least one dimension";

                // Optimize common single-element cases
                if (size == 1) {
                    if (buffer[0] == 0) {
                        return SINGLE_ZERO_SHAPE;
                    } else if (buffer[0] == 1) {
                        return SINGLE_ONE_SHAPE;
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
        }
    }

    /**
     * Represents a completed inference response with metadata for result building.
     *
     * @param inferenceResponse The inference response (may be null for null requests).
     * @param shape             The shape array from the corresponding request.
     * @param seqNo             The sequence number for ordering.
     */
    public record BulkInferenceResponseItem(InferenceAction.Response inferenceResponse, int[] shape, long seqNo) {}

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
                if (requestItemIterator.hasNext() == false) {
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
