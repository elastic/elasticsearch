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

    public BulkInferenceExecutionState(int bufferSize) {
        this.bufferedResponses = new ConcurrentHashMap<>(bufferSize);
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
     * * Handles an exception thrown during inference execution.
     *  Records the failure and marks the corresponding sequence number as processed.
     *
     *  @param seqNo The sequence number of the inference request.
     *  @param e     The exception
     */
    public synchronized void onInferenceException(long seqNo, Exception e) {
        failureCollector.unwrapAndCollect(e);
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

    public void addFailure(Exception e) {
        failureCollector.unwrapAndCollect(e);
    }

    /**
     * Indicates whether the entire bulk execution is marked as finished and all responses have been successfully persisted.
     */
    public boolean finished() {
        return finished.get() && getMaxSeqNo() == getPersistedCheckpoint();
    }

    /**
     * Marks the bulk as finished, indicating that all inference requests have been sent.
     */
    public void finish() {
        this.finished.set(true);
    }
}
