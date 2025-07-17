/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.esql.inference.InferenceRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a sequence of inference requests in bulk with throttling and concurrency control.
 */
public class BulkInferenceRunner {

    private final InferenceRunner inferenceRunner;

    /**
     * Constructs a new {@code BulkInferenceRunner}.
     *
     * @param inferenceRunner The throttled inference runner used to execute individual inference requests.
     */
    public BulkInferenceRunner(InferenceRunner inferenceRunner) {
        this.inferenceRunner = inferenceRunner;
    }

    /**
     * Executes the provided bulk inference requests.
     * <p>
     * Each request is sent to the {@link InferenceRunner} to be executed.
     * The final listener is notified with all successful responses once all requests are completed.
     * </p>
     *
     * @param requests An iterator over the inference requests to be executed.
     * @param listener A listener notified with the complete list of responses or a failure.
     */
    public void execute(BulkInferenceRequestIterator requests, ActionListener<List<InferenceAction.Response>> listener) {
        if (requests.hasNext() == false) {
            listener.onResponse(List.of());
            return;
        }

        final BulkInferenceExecutionState bulkExecutionState = new BulkInferenceExecutionState();
        final ResponseHandler responseHandler = new ResponseHandler(bulkExecutionState, listener, requests.estimatedSize());

        while (bulkExecutionState.finished() == false && requests.hasNext()) {
            InferenceAction.Request request = requests.next();
            long seqNo = bulkExecutionState.generateSeqNo();

            if (requests.hasNext() == false) {
                bulkExecutionState.finish();
            }

            ActionListener<InferenceAction.Response> inferenceResponseListener = ActionListener.runAfter(
                ActionListener.wrap(
                    r -> bulkExecutionState.onInferenceResponse(seqNo, r),
                    e -> bulkExecutionState.onInferenceException(seqNo, e)
                ),
                responseHandler::persistPendingResponses
            );

            if (request == null) {
                inferenceResponseListener.onResponse(null);
            } else {
                inferenceRunner.execute(request, inferenceResponseListener);
            }
        }
    }

    /**
     * Handles collection and delivery of inference responses once they are complete.
     */
    private static class ResponseHandler {
        private final List<InferenceAction.Response> responses;
        private final ActionListener<List<InferenceAction.Response>> listener;
        private final BulkInferenceExecutionState bulkExecutionState;
        private final AtomicBoolean responseSent = new AtomicBoolean(false);

        private ResponseHandler(
            BulkInferenceExecutionState bulkExecutionState,
            ActionListener<List<InferenceAction.Response>> listener,
            int estimatedSize
        ) {
            this.listener = listener;
            this.bulkExecutionState = bulkExecutionState;
            this.responses = new ArrayList<>(estimatedSize);
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
                        responses.add(response);
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
                        listener.onResponse(responses);
                        return;
                    } catch (Exception e) {
                        bulkExecutionState.addFailure(e);
                    }
                }

                listener.onFailure(bulkExecutionState.getFailure());
            }
        }
    }
}
