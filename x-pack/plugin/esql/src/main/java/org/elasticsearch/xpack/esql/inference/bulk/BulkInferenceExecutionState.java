/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class BulkInferenceExecutionState {
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final FailureCollector failureCollector = new FailureCollector();
    private final AtomicBoolean responseSent = new AtomicBoolean(false);
    private final AtomicBoolean allRequestsSent = new AtomicBoolean(false);
    final Map<Long, InferenceAction.Response> bufferedResponses = new ConcurrentHashMap<>();

    public long generateSeqNo() {
        return checkpoint.generateSeqNo();
    }

    public void onInferenceResponse(long seqNo, InferenceAction.Response response) {
        if (failureCollector.hasFailure() == false) {
            bufferedResponses.put(seqNo, response);
        }
        checkpoint.markSeqNoAsProcessed(seqNo);
    }

    public void onInferenceException(long seqNo, Exception e) {
        failureCollector.unwrapAndCollect(e);
        checkpoint.markSeqNoAsProcessed(seqNo);
    }

    public void markSeqNoAsPersisted(long seqNo) {
        checkpoint.markSeqNoAsPersisted(seqNo);
    }

    public void persistsResponses(CheckedConsumer<InferenceAction.Response, Exception> persister) {
        synchronized (checkpoint) {
            long persistedSeqNo = checkpoint.getPersistedCheckpoint();
            while (persistedSeqNo < checkpoint.getProcessedCheckpoint()) {
                persistedSeqNo++;
                InferenceAction.Response response = bufferedResponses.remove(persistedSeqNo);
                assert response != null || hasFailure();
                if (hasFailure() == false && responseSent() == false) {
                    try {
                        persister.accept(response);
                    } catch (Exception e) {
                        failureCollector.unwrapAndCollect(e);
                    }
                }
                checkpoint.markSeqNoAsPersisted(persistedSeqNo);
            }
        }
    }

    private boolean hasFailure() {
        return failureCollector.hasFailure();
    }

    public Exception getFailure() {
        return failureCollector.getFailure();
    }

    public void addFailure(Exception e) {
        failureCollector.unwrapAndCollect(e);
    }

    public <OutputType> void maybeSendResponse(CheckedSupplier<OutputType, Exception> responseBuilder, ActionListener<OutputType> l) {
        if (allRequestsSent() == false) {
            return;
        }

        if (checkpoint.getPersistedCheckpoint() < checkpoint.getMaxSeqNo()) {
            return;
        }

        if (responseSent.compareAndSet(false, true)) {
            if (failureCollector.hasFailure() == false) {
                try {
                    l.onResponse(responseBuilder.get());
                    return;
                } catch (Exception e) {
                    l.onFailure(e);
                    return;
                }
            }

            l.onFailure(failureCollector.getFailure());
        }
    }

    public boolean responseSent() {
        return responseSent.get();
    }

    public void markAllRequestsSent() {
        allRequestsSent.set(true);
    }

    public boolean allRequestsSent() {
        return allRequestsSent.get();
    }
}
