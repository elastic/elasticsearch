/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link AsyncOperator} performs an external computation specified in {@link #performAsync(Page, ActionListener)}.
 * This operator acts as a client and operates on a per-page basis to reduce communication overhead.
 * @see #performAsync(Page, ActionListener)
 */
public abstract class AsyncOperator implements Operator {

    private volatile ListenableActionFuture<Void> blockedFuture;

    private final Map<Long, Page> buffers = ConcurrentCollections.newConcurrentMap();
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private final int maxOutstandingRequests;
    private boolean finished = false;

    /*
     * The checkpoint tracker is used to maintain the order of emitted pages after passing through this async operator.
     * - Generates a new sequence number for each incoming page
     * - Uses the processed checkpoint for pages that have completed this computation
     * - Uses the persisted checkpoint for pages that have already been emitted to the next operator
     */
    private final LocalCheckpointTracker checkpoint = new LocalCheckpointTracker(
        SequenceNumbers.NO_OPS_PERFORMED,
        SequenceNumbers.NO_OPS_PERFORMED
    );

    /**
     * Create an operator that performs an external computation
     *
     * @param maxOutstandingRequests the maximum number of outstanding requests
     */
    public AsyncOperator(int maxOutstandingRequests) {
        this.maxOutstandingRequests = maxOutstandingRequests;
    }

    @Override
    public boolean needsInput() {
        final long outstandingPages = checkpoint.getMaxSeqNo() - checkpoint.getPersistedCheckpoint();
        return outstandingPages < maxOutstandingRequests;
    }

    @Override
    public void addInput(Page input) {
        checkFailure();
        final long seqNo = checkpoint.generateSeqNo();
        performAsync(input, ActionListener.wrap(output -> {
            buffers.put(seqNo, output);
            onSeqNoCompleted(seqNo);
        }, e -> {
            onFailure(e);
            onSeqNoCompleted(seqNo);
        }));
    }

    /**
     * Performs an external computation and notify the listener when the result is ready.
     *
     * @param inputPage the input page
     * @param listener  the listener
     */
    protected abstract void performAsync(Page inputPage, ActionListener<Page> listener);

    private void onFailure(Exception e) {
        failure.getAndUpdate(first -> {
            if (first == null) {
                return e;
            }
            // ignore subsequent TaskCancelledException exceptions as they don't provide useful info.
            if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
                return first;
            }
            if (ExceptionsHelper.unwrap(first, TaskCancelledException.class) != null) {
                return e;
            }
            if (ExceptionsHelper.unwrapCause(first) != ExceptionsHelper.unwrapCause(e)) {
                first.addSuppressed(e);
            }
            return first;
        });
    }

    private void onSeqNoCompleted(long seqNo) {
        checkpoint.markSeqNoAsProcessed(seqNo);
        if (checkpoint.getPersistedCheckpoint() < checkpoint.getProcessedCheckpoint()) {
            notifyIfBlocked();
        }
    }

    private void notifyIfBlocked() {
        if (blockedFuture != null) {
            final ListenableActionFuture<Void> future;
            synchronized (this) {
                future = blockedFuture;
                this.blockedFuture = null;
            }
            if (future != null) {
                future.onResponse(null);
            }
        }
    }

    private void checkFailure() {
        Exception e = failure.get();
        if (e != null) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        checkFailure();
        return finished && checkpoint.getPersistedCheckpoint() == checkpoint.getMaxSeqNo();
    }

    @Override
    public Page getOutput() {
        checkFailure();
        long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
        if (persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
            persistedCheckpoint++;
            Page page = buffers.remove(persistedCheckpoint);
            checkpoint.markSeqNoAsPersisted(persistedCheckpoint);
            return page;
        } else {
            return null;
        }
    }

    @Override
    public ListenableActionFuture<Void> isBlocked() {
        if (finished) {
            return Operator.NOT_BLOCKED;
        }
        long persistedCheckpoint = checkpoint.getPersistedCheckpoint();
        if (persistedCheckpoint == checkpoint.getMaxSeqNo() || persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
            return Operator.NOT_BLOCKED;
        }
        synchronized (this) {
            persistedCheckpoint = checkpoint.getPersistedCheckpoint();
            if (persistedCheckpoint == checkpoint.getMaxSeqNo() || persistedCheckpoint < checkpoint.getProcessedCheckpoint()) {
                return Operator.NOT_BLOCKED;
            }
            if (blockedFuture == null) {
                blockedFuture = new ListenableActionFuture<>();
            }
            return blockedFuture;
        }
    }
}
