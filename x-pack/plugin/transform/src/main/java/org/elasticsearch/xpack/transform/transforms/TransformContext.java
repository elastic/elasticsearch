/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.transform.transforms.DataFrameTransformTaskState;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TransformContext {

    private interface TaskCallbacks {
        void shutdown();

        void fail(String failureMessage, ActionListener<Void> listener);
    }

    private final AtomicReference<DataFrameTransformTaskState> taskState;
    private final AtomicReference<String> stateReason;

    // the checkpoint of this transform, storing the checkpoint until data indexing from source to dest is _complete_
    // Note: Each indexer run creates a new future checkpoint which becomes the current checkpoint only after the indexer run finished
    private final AtomicLong currentCheckpoint;

    TransformContext (final DataFrameTransformTaskState taskState, String stateReason, long currentCheckpoint) {
        this.taskState = new AtomicReference<>(taskState);
        this.stateReason = new AtomicReference<>(stateReason);
        this.currentCheckpoint = new AtomicLong(currentCheckpoint);
    }

    DataFrameTransformTaskState getTaskState() {
        return taskState.get();
    }

    void setTaskState(DataFrameTransformTaskState newState) {
        taskState.set(newState);
    }

    void setTaskState(DataFrameTransformTaskState oldState, DataFrameTransformTaskState newState) {
        taskState.compareAndSet(oldState, newState);
    }

    void resetTaskState() {
        taskState.set(DataFrameTransformTaskState.STARTED);
        stateReason.set(null);
    }

    void setTaskStateToFailed(String reason) {
        taskState.set(DataFrameTransformTaskState.FAILED);
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

}
