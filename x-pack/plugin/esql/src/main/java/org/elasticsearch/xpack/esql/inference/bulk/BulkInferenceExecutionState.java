/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

/**
 * Tracks the state of a bulk inference execution, including sequencing, failure management, and buffering of inference responses for
 * ordered output construction.
 */
public class BulkInferenceExecutionState {
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final FailureCollector failureCollector = new FailureCollector();
    private final Map<Long, InferenceAction.Response> bufferedResponses;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public BulkInferenceExecutionState() {
        this.bufferedResponses = new ConcurrentHashMap<>();
    }

    /**
     * Generates a new unique sequence number for an inference request.
     */
    public long generateSeqNo() {
        return checkpoint.generateSeqNo();
    }

    /**
     * Returns the highest sequence number marked as persisted, such that all lower sequence numbers have also been marked as persisted.
     */
    public long getPersistedCheckpoint() {
        return checkpoint.getPersistedCheckpoint();
    }

    /**
     * Returns the highest sequence number marked as processed, such that all lower sequence numbers have also been marked as processed.
     */
    public long getProcessedCheckpoint() {
        return checkpoint.getProcessedCheckpoint();
    }

    /**
     * Highest generated sequence number.
     */
    public long getMaxSeqNo() {
        return checkpoint.getMaxSeqNo();
    }

    /**
     * Marks an inference response as persisted.
     *
     * @param seqNo The corresponding sequence number
     */
    public void markSeqNoAsPersisted(long seqNo) {
        checkpoint.markSeqNoAsPersisted(seqNo);
    }

    /**
     *  Add an inference response to the buffer and marks the corresponding sequence number as processed.
     *
     *  @param seqNo    The sequence number of the inference request.
     *  @param response The inference response.
     */
    public synchronized void onInferenceResponse(long seqNo, InferenceAction.Response response) {
        if (response != null && failureCollector.hasFailure() == false) {
            bufferedResponses.put(seqNo, response);
        }
        checkpoint.markSeqNoAsProcessed(seqNo);
    }

    /**
     * Records an inference exception for a specific sequence number.
     * <p>
     * When any inference request fails, this method:
     * 1. Adds the failure to the failure collector (which automatically finishes the bulk operation)
     * 2. Marks the sequence number as processed to maintain checkpoint consistency
     * 3. Clears all buffered responses since the bulk operation has failed
     * </p>
     * <p>
     * The fail-fast behavior ensures that once any request fails, the entire bulk operation
     * terminates quickly rather than continuing to process remaining requests.
     * </p>
     *
     * @param seqNo The sequence number of the failed request
     * @param e     The exception that occurred during inference
     */
    public synchronized void onInferenceException(long seqNo, Exception e) {
        addFailure(e);  // This automatically calls finish() to terminate the bulk operation
        checkpoint.markSeqNoAsProcessed(seqNo);
        bufferedResponses.clear();
    }

    /**
     * Retrieves and removes the buffered response by  sequence number.
     *
     * @param seqNo The sequence number of the response to fetch.
     */
    public synchronized InferenceAction.Response fetchBufferedResponse(long seqNo) {
        return bufferedResponses.remove(seqNo);
    }

    /**
     * Returns whether any failure has been recorded during execution.
     */
    public boolean hasFailure() {
        return failureCollector.hasFailure();
    }

    /**
     * Returns the recorded failure, if any.
     */
    public Exception getFailure() {
        return failureCollector.getFailure();
    }

    /**
     * Adds a failure to the bulk execution and immediately terminates the operation.
     * <p>
     * This method implements fail-fast behavior by:
     * 1. Recording the failure in the failure collector (which unwraps and processes the exception)
     * 2. Immediately marking the bulk execution as finished to prevent further processing
     * </p>
     * <p>
     * The automatic finishing ensures that once any error occurs, the bulk operation
     * terminates quickly and resources are cleaned up promptly.
     * </p>
     *
     * @param e The exception to record as a failure
     */
    public void addFailure(Exception e) {
        failureCollector.unwrapAndCollect(e);
        finish();  // Immediately terminate the bulk operation on any failure
    }

    /**
     * Indicates whether the entire bulk execution is marked as finished and all responses have been successfully persisted.
     */
    public boolean finished() {
        return finished.get() && (failureCollector.hasFailure() || getMaxSeqNo() == getPersistedCheckpoint());
    }

    /**
     * Marks the bulk as finished, indicating that all inference requests have been sent.
     */
    public void finish() {
        this.finished.set(true);
    }
}
