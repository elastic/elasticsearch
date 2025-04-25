/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class BulkInferenceOperation {
    private final Iterator<InferenceAction.Request> requests;
    private final CheckedConsumer<InferenceAction.Response, ?> responseConsumer;
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final FailureCollector failureCollector = new FailureCollector();
    private final Map<Long, InferenceAction.Response> bufferedResponses = new ConcurrentHashMap<>();
    private final AtomicBoolean responseSent = new AtomicBoolean(false);

    public BulkInferenceOperation(
        Iterator<InferenceAction.Request> requests,
        CheckedConsumer<InferenceAction.Response, ?> responseConsumer
    ) {
        this.requests = requests;
        this.responseConsumer = responseConsumer;
    }

    public void execute(InferenceExecutionContext ctx, ActionListener<Void> completionListener) {
        int threadCount = 0;
        while (threadCount++ < ctx.maxConcurrentRequests()) {
            ctx.executorService().submit(() -> {
                while (true) {
                    BulkInferenceRequestItem bulkItemRequest = nextRequest();
                    if (bulkItemRequest == null) {
                        break;
                    }
                    execute(bulkItemRequest, ctx, () -> onResponseProcessed(completionListener));
                }
            });
        }
    }

    private void execute(BulkInferenceRequestItem bulkItemRequest, InferenceExecutionContext ctx, Runnable onResponseProcessed) {
        final ActionListener<InferenceAction.Response> responseListener = ActionListener.wrap(
            inferenceResponse -> onInferenceResponse(bulkItemRequest.seqNo(), inferenceResponse),
            exception -> onInferenceException(bulkItemRequest.seqNo(), exception)
        );

        ctx.inferenceRunner().doInference(bulkItemRequest.request(), ActionListener.runAfter(responseListener, onResponseProcessed));
    }

    private boolean isCompleted() {
        return requests.hasNext() == false && checkpoint.getMaxSeqNo() == checkpoint.getPersistedCheckpoint();
    }

    private void onResponseProcessed(ActionListener<Void> completionListener) {
        if (isCompleted() && responseSent.compareAndSet(false, true)) {
            if (failureCollector.hasFailure()) {
                completionListener.onFailure(failureCollector.getFailure());
                return;
            }
            completionListener.onResponse(null);
        }
    }

    private BulkInferenceRequestItem nextRequest() {
        synchronized (checkpoint) {
            if (requests.hasNext()) {
                return new BulkInferenceRequestItem(checkpoint.generateSeqNo(), requests.next());
            }

            return null;
        }
    }

    private void onInferenceResponse(long seqNo, InferenceAction.Response response) {
        if (failureCollector.hasFailure() == false) {
            bufferedResponses.put(seqNo, response);
        }
        checkpoint.markSeqNoAsProcessed(seqNo);

        synchronized (checkpoint) {
            long persistSeqNo = checkpoint.getPersistedCheckpoint();
            while (persistSeqNo < checkpoint.getProcessedCheckpoint()) {
                persistSeqNo++;
                if (failureCollector.hasFailure() == false) {
                    try {
                        responseConsumer.accept(bufferedResponses.remove(persistSeqNo));
                    } catch (Exception e) {
                        failureCollector.unwrapAndCollect(e);
                    }
                }
                checkpoint.markSeqNoAsPersisted(persistSeqNo);
            }
        }
    }

    public void onInferenceException(long seqNo, Exception e) {
        failureCollector.unwrapAndCollect(e);
        checkpoint.markSeqNoAsProcessed(seqNo);

        synchronized (checkpoint) {
            long persistSeqNo = checkpoint.getPersistedCheckpoint();
            while (persistSeqNo < checkpoint.getProcessedCheckpoint()) {
                persistSeqNo++;
                bufferedResponses.remove(persistSeqNo);
                checkpoint.markSeqNoAsPersisted(persistSeqNo);
            }
        }
    }

    private record BulkInferenceRequestItem(long seqNo, InferenceAction.Request request) {}
}
