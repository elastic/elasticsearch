/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class BulkInferenceExecutionState {
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(NO_OPS_PERFORMED, NO_OPS_PERFORMED);
    private final FailureCollector failureCollector = new FailureCollector();
    private final Map<Long, InferenceAction.Response> bufferedResponses = new ConcurrentHashMap<>();
    private final BlockingQueue<Long> processedSeqNoQueue = ConcurrentCollections.newBlockingQueue();
    private final AtomicBoolean finished = new AtomicBoolean(false);

    public long generateSeqNo() {
        return checkpoint.generateSeqNo();
    }

    public long getPersistedCheckpoint() {
        return checkpoint.getPersistedCheckpoint();
    }

    public long getProcessedCheckpoint() {
        return checkpoint.getProcessedCheckpoint();
    }

    public long getMaxSeqNo() {
        return checkpoint.getMaxSeqNo();
    }

    public void onInferenceResponse(long seqNo, InferenceAction.Response response) {
        if (failureCollector.hasFailure() == false) {
            bufferedResponses.put(seqNo, response);
        }
        checkpoint.markSeqNoAsProcessed(seqNo);
        processedSeqNoQueue.offer(seqNo);
    }

    public void onInferenceException(long seqNo, Exception e) {
        failureCollector.unwrapAndCollect(e);
        checkpoint.markSeqNoAsProcessed(seqNo);
        processedSeqNoQueue.offer(seqNo);
    }

    public long fetchProcessedSeqNo(int retry) throws InterruptedException, TimeoutException {
        while (retry > 0) {
            if (finished()) {
                return -1;
            }
            retry--;
            Long seqNo = processedSeqNoQueue.poll(1, TimeUnit.SECONDS);
            if (seqNo != null) {
                return seqNo;
            }
        }

        throw new TimeoutException("timeout waiting for inference response");
    }

    public InferenceAction.Response fetchBufferedResponse(long seqNo) {
        return bufferedResponses.remove(seqNo);
    }

    public void markSeqNoAsPersisted(long seqNo) {
        checkpoint.markSeqNoAsPersisted(seqNo);
    }

    public boolean hasFailure() {
        return failureCollector.hasFailure();
    }

    public Exception getFailure() {
        return failureCollector.getFailure();
    }

    public void addFailure(Exception e) {
        failureCollector.unwrapAndCollect(e);
    }

    public boolean finished() {
        return finished.get() && getMaxSeqNo() == getPersistedCheckpoint();
    }

    public void finish() {
        this.finished.set(true);
    }
}
