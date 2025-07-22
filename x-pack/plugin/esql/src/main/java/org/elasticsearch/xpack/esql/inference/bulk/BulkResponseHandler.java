/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handles collection and delivery of inference responses once they are complete.
 * <p>
 * This class manages the state of bulk inference execution, ensuring that responses
 * are delivered in the correct order and that the completion listener is only
 * invoked once all responses have been processed.
 */
public class BulkResponseHandler {

    private final Consumer<InferenceAction.Response> responseConsumer;
    private final ActionListener<Void> completionListener;
    private final BulkInferenceExecutionState bulkExecutionState;
    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    public BulkResponseHandler(
        BulkInferenceExecutionState bulkExecutionState,
        Consumer<InferenceAction.Response> responseConsumer,
        ActionListener<Void> completionListener

    ) {
        this.responseConsumer = responseConsumer;
        this.completionListener = completionListener;
        this.bulkExecutionState = bulkExecutionState;
    }

    /**
     * Persists all buffered responses that can be delivered in order, and sends the final response if all requests are finished.
     */
    public synchronized void persistPendingResponses() {
        long persistedSeqNo = bulkExecutionState.getPersistedCheckpoint();

        while (persistedSeqNo < bulkExecutionState.getProcessedCheckpoint()) {
            persistedSeqNo++;
            if (bulkExecutionState.hasFailure() == false) {
                try {
                    InferenceAction.Response response = bulkExecutionState.fetchBufferedResponse(persistedSeqNo);
                    responseConsumer.accept(response);
                } catch (Exception e) {
                    bulkExecutionState.addFailure(e);
                }
            }
            bulkExecutionState.markSeqNoAsPersisted(persistedSeqNo);
        }

        sendResponseOnCompletion();
    }

    /**
     * Sends the final response or failure once all inference tasks have completed.
     */
    private void sendResponseOnCompletion() {
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
}
