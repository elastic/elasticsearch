/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TransformContext {

    public interface Listener {
        void shutdown();

        void failureCountChanged();

        void fail(Throwable exception, String failureMessage, ActionListener<Void> listener);
    }

    private final AtomicReference<TransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    private volatile Instant stateFailureTime;
    private final Listener taskListener;
    private volatile int numFailureRetries = Transform.DEFAULT_FAILURE_RETRIES;
    private final AtomicInteger failureCount;
    // Keeps track of the last failure that occurred, used for throttling logs and audit
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();
    private volatile Instant lastFailureStartTime;
    private final AtomicInteger statePersistenceFailureCount = new AtomicInteger();
    private final AtomicReference<Throwable> lastStatePersistenceFailure = new AtomicReference<>();
    private volatile Instant lastStatePersistenceFailureStartTime;
    private final AtomicInteger startUpFailureCount = new AtomicInteger();
    private final AtomicReference<Throwable> lastStartUpFailure = new AtomicReference<>();
    private volatile Instant startUpFailureTime;
    private volatile Instant changesLastDetectedAt;
    private volatile Instant lastSearchTime;
    private volatile boolean shouldStopAtCheckpoint = false;
    private volatile boolean shouldRecreateDestinationIndex = false;
    private volatile AuthorizationState authState;
    private volatile int pageSize = 0;

    /**
     * If the destination index is blocked (e.g. during a reindex), the Transform will fail to write to it.
     * {@link TransformFailureHandler} will silence the error so the Transform automatically retries.
     * Every time the Transform runs, it will check if the index is unblocked and reset this to false.
     * Users can override this via the `_schedule_now` API.
     */
    private volatile boolean isWaitingForIndexToUnblock = false;

    // the checkpoint of this transform, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;

    private final Instant from;

    public TransformContext(TransformTaskState taskState, String stateReason, long currentCheckpoint, Listener taskListener) {
        this(taskState, stateReason, currentCheckpoint, null, taskListener);
    }

    public TransformContext(TransformTaskState taskState, String stateReason, long currentCheckpoint, Instant from, Listener taskListener) {
        this.taskState = new AtomicReference<>(taskState);
        this.stateReason = new AtomicReference<>(stateReason);
        this.currentCheckpoint = new AtomicLong(currentCheckpoint);
        this.from = from;
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
        stateFailureTime = Instant.now();
    }

    void resetReasonAndFailureCounter() {
        stateReason.set(null);
        failureCount.set(0);
        lastFailure.set(null);
        stateFailureTime = null;
        lastFailureStartTime = null;
        taskListener.failureCountChanged();
    }

    String getStateReason() {
        return stateReason.get();
    }

    Instant getStateFailureTime() {
        return stateFailureTime;
    }

    void setCheckpoint(long newValue) {
        currentCheckpoint.set(newValue);
    }

    long getCheckpoint() {
        return currentCheckpoint.get();
    }

    Instant from() {
        return from;
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

    int incrementAndGetFailureCount(Throwable failure) {
        int newFailureCount = failureCount.incrementAndGet();
        lastFailure.set(failure);
        if (newFailureCount == 1) {
            lastFailureStartTime = Instant.now();
        }

        taskListener.failureCountChanged();
        return newFailureCount;
    }

    Throwable getLastFailure() {
        return lastFailure.get();
    }

    Instant getLastFailureStartTime() {
        return lastFailureStartTime;
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

    public boolean shouldRecreateDestinationIndex() {
        return shouldRecreateDestinationIndex;
    }

    public void setShouldRecreateDestinationIndex(boolean shouldRecreateDestinationIndex) {
        this.shouldRecreateDestinationIndex = shouldRecreateDestinationIndex;
    }

    public boolean isWaitingForIndexToUnblock() {
        return isWaitingForIndexToUnblock;
    }

    public void setIsWaitingForIndexToUnblock(boolean isWaitingForIndexToUnblock) {
        this.isWaitingForIndexToUnblock = isWaitingForIndexToUnblock;
    }

    public AuthorizationState getAuthState() {
        return authState;
    }

    public void setAuthState(AuthorizationState authState) {
        this.authState = authState;
    }

    int getPageSize() {
        return pageSize;
    }

    void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    void resetStatePersistenceFailureCount() {
        statePersistenceFailureCount.set(0);
        lastStatePersistenceFailure.set(null);
        lastStatePersistenceFailureStartTime = null;
    }

    int getStatePersistenceFailureCount() {
        return statePersistenceFailureCount.get();
    }

    Throwable getLastStatePersistenceFailure() {
        return lastStatePersistenceFailure.get();
    }

    int incrementAndGetStatePersistenceFailureCount(Throwable failure) {
        lastStatePersistenceFailure.set(failure);
        int newFailureCount = statePersistenceFailureCount.incrementAndGet();
        if (newFailureCount == 1) {
            lastStatePersistenceFailureStartTime = Instant.now();
        }
        return newFailureCount;
    }

    Instant getLastStatePersistenceFailureStartTime() {
        return lastStatePersistenceFailureStartTime;
    }

    void resetStartUpFailureCount() {
        startUpFailureCount.set(0);
        lastStartUpFailure.set(null);
        startUpFailureTime = null;
    }

    int getStartUpFailureCount() {
        return startUpFailureCount.get();
    }

    Throwable getStartUpFailure() {
        return lastStartUpFailure.get();
    }

    int incrementAndGetStartUpFailureCount(Throwable failure) {
        lastStartUpFailure.set(failure);
        int newFailureCount = startUpFailureCount.incrementAndGet();
        if (newFailureCount == 1) {
            startUpFailureTime = Instant.now();
        }
        return newFailureCount;
    }

    Instant getStartUpFailureTime() {
        return startUpFailureTime;
    }

    boolean doesNotHaveFailures() {
        return getFailureCount() == 0 && getStatePersistenceFailureCount() == 0 && getStartUpFailureCount() == 0;
    }

    void shutdown() {
        taskListener.shutdown();
    }

    void markAsFailed(Throwable exception, String failureMessage) {
        taskListener.fail(exception, failureMessage, ActionListener.wrap(r -> {
            // Successfully marked as failed, reset counter so that task can be restarted
            failureCount.set(0);
        }, e -> {}));
    }
}
