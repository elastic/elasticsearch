/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TransformContext {

    public interface Listener {
        void shutdown();

        void fail(String failureMessage, ActionListener<Void> listener);
    }

    private final AtomicReference<TransformTaskState> taskState;
    private final AtomicReference<String> stateReason;
    private final Listener taskListener;
    private volatile int numFailureRetries = Transform.DEFAULT_FAILURE_RETRIES;

    // the checkpoint of this transform, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;

    TransformContext (final TransformTaskState taskState, String stateReason, long currentCheckpoint, Listener taskListener) {
        this.taskState = new AtomicReference<>(taskState);
        this.stateReason = new AtomicReference<>(stateReason);
        this.currentCheckpoint = new AtomicLong(currentCheckpoint);
        this.taskListener = taskListener;
    }

    TransformTaskState getTaskState() {
        return taskState.get();
    }

    void setTaskState(TransformTaskState newState) {
        taskState.set(newState);
    }

    void setTaskState(TransformTaskState oldState, TransformTaskState newState) {
        taskState.compareAndSet(oldState, newState);
    }

    void resetTaskState() {
        taskState.set(TransformTaskState.STARTED);
        stateReason.set(null);
    }

    void setTaskStateToFailed(String reason) {
        taskState.set(TransformTaskState.FAILED);
        stateReason.set(reason);
    }

    void setStateReason(String reason) {
        stateReason.set(reason);
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

    long incrementCheckpoint() {
        return currentCheckpoint.getAndIncrement();
    }

    void setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
    }

    int getNumFailureRetries() {
        return numFailureRetries;
    }

    void shutdown() {
        taskListener.shutdown();
    }

    void markAsFailed(String failureMessage, ActionListener<Void> listener) {
        taskListener.fail(failureMessage, listener);
    }

}
