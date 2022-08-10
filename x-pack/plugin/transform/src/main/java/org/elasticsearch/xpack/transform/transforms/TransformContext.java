/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TransformContext {

    public interface Listener {
        void shutdown();

        void failureCountChanged();

        void fail(String failureMessage, ActionListener<Void> listener);
    }

    private final AtomicReference<TransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    private final Listener taskListener;
    private volatile int numFailureRetries = Transform.DEFAULT_FAILURE_RETRIES;
    private final AtomicInteger failureCount;
    // Keeps track of the last failure that occured, used for throttling logs and audit
    private final AtomicReference<String> lastFailure = new AtomicReference<>();
    private final AtomicInteger statePersistenceFailureCount = new AtomicInteger();
    private volatile Instant changesLastDetectedAt;
    private volatile Instant lastSearchTime;
    private volatile boolean shouldStopAtCheckpoint = false;
    private volatile int pageSize = 0;

    // the checkpoint of this transform, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;

    TransformContext(TransformTaskState taskState, String stateReason, long currentCheckpoint, Listener taskListener) {
        this.taskState = new AtomicReference<>(taskState);
        this.stateReason = new AtomicReference<>(stateReason);
        this.currentCheckpoint = new AtomicLong(currentCheckpoint);
        this.taskListener = taskListener;
        this.failureCount = new AtomicInteger(0);
    }

    TransformTaskState getTaskState() {
        return taskState.get();
    }

    boolean setTaskState(TransformTaskState oldState, TransformTaskState newState) {
        return taskState.compareAndSet(oldState, newState);
    }

    void resetTaskState() {
        taskState.set(TransformTaskState.STARTED);
        stateReason.set(null);
    }

    void setTaskStateToFailed(String reason) {
        taskState.set(TransformTaskState.FAILED);
        stateReason.set(reason);
    }

    void resetReasonAndFailureCounter() {
        stateReason.set(null);
        failureCount.set(0);
        lastFailure.set(null);
        taskListener.failureCountChanged();
    }

    String getStateReason() {
        return stateReason.get();
    }

    void setCheckpoint(long newValue) {
        currentCheckpoint.set(newValue);
    }

    long getCheckpoint() {
        return currentCheckpoint.get();
    }

    long incrementAndGetCheckpoint() {
        return currentCheckpoint.incrementAndGet();
    }

    void setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
    }

    int getNumFailureRetries() {
        return numFailureRetries;
    }

    int getFailureCount() {
        return failureCount.get();
    }

    int incrementAndGetFailureCount(String failure) {
        int newFailureCount = failureCount.incrementAndGet();
        lastFailure.set(failure);
        taskListener.failureCountChanged();
        return newFailureCount;
    }

    String getLastFailure() {
        return lastFailure.get();
    }

    void setChangesLastDetectedAt(Instant time) {
        changesLastDetectedAt = time;
    }

    Instant getChangesLastDetectedAt() {
        return changesLastDetectedAt;
    }

    void setLastSearchTime(Instant time) {
        lastSearchTime = time;
    }

    Instant getLastSearchTime() {
        return lastSearchTime;
    }

    public boolean shouldStopAtCheckpoint() {
        return shouldStopAtCheckpoint;
    }

    public void setShouldStopAtCheckpoint(boolean shouldStopAtCheckpoint) {
        this.shouldStopAtCheckpoint = shouldStopAtCheckpoint;
    }

    int getPageSize() {
        return pageSize;
    }

    void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    void resetStatePersistenceFailureCount() {
        statePersistenceFailureCount.set(0);
    }

    int getStatePersistenceFailureCount() {
        return statePersistenceFailureCount.get();
    }

    int incrementAndGetStatePersistenceFailureCount() {
        return statePersistenceFailureCount.incrementAndGet();
    }

    void shutdown() {
        taskListener.shutdown();
    }

    void markAsFailed(String failureMessage) {
        taskListener.fail(failureMessage, ActionListener.wrap(r -> {
            // Successfully marked as failed, reset counter so that task can be restarted
            failureCount.set(0);
        }, e -> {}));
    }
}
