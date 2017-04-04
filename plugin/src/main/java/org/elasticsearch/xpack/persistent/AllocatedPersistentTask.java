/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a executor node operation that corresponds to a persistent task
 */
public class AllocatedPersistentTask extends CancellableTask {
    private long persistentTaskId;
    private long allocationId;

    private final AtomicReference<State> state;
    @Nullable
    private Exception failure;

    private PersistentTasksService persistentTasksService;


    public AllocatedPersistentTask(long id, String type, String action, String description, TaskId parentTask) {
        super(id, type, action, description, parentTask);
        this.state = new AtomicReference<>(State.STARTED);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    // In case of persistent tasks we always need to return: `false`
    // because in case of persistent task the parent task isn't a task in the task manager, but in cluster state.
    // This instructs the task manager not to try to kill this persistent task when the task manager cannot find
    // a fake parent node id "cluster" in the cluster state
    @Override
    public final boolean cancelOnParentLeaving() {
        return false;
    }

    @Override
    public Status getStatus() {
        return new PersistentTasksNodeService.Status(state.get());
    }

    /**
     * Updates the persistent state for the corresponding persistent task. 
     * 
     * This doesn't affect the status of this allocated task. 
     */
    public void updatePersistentStatus(Task.Status status, ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>> listener) {
        persistentTasksService.updateStatus(persistentTaskId, allocationId, status, listener);
    }

    public long getPersistentTaskId() {
        return persistentTaskId;
    }

    void init(PersistentTasksService persistentTasksService, long persistentTaskId, long allocationId) {
        this.persistentTasksService = persistentTasksService;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
    }
    
    public Exception getFailure() {
        return failure;
    }

    boolean startNotification(Exception failure) {
        boolean result = state.compareAndSet(AllocatedPersistentTask.State.STARTED, AllocatedPersistentTask.State.FAILED);
        if (result) {
            this.failure = failure;
        }
        return result;
    }

    boolean notificationFailed() {
        return state.compareAndSet(AllocatedPersistentTask.State.FAILED, AllocatedPersistentTask.State.FAILED_NOTIFICATION);
    }

    boolean restartCompletionNotification() {
        return state.compareAndSet(AllocatedPersistentTask.State.FAILED_NOTIFICATION, AllocatedPersistentTask.State.FAILED);
    }

    boolean markAsNotified() {
        return state.compareAndSet(AllocatedPersistentTask.State.FAILED, AllocatedPersistentTask.State.NOTIFIED);
    }

    boolean markAsCancelled() {
        return state.compareAndSet(AllocatedPersistentTask.State.STARTED, AllocatedPersistentTask.State.CANCELLED);
    }

    public State getState() {
        return state.get();
    }

    public enum State {
        STARTED,  // the task is currently running
        CANCELLED, // the task is cancelled
        FAILED,     // the task is done running and trying to notify caller
        FAILED_NOTIFICATION, // the caller notification failed
        NOTIFIED // the caller was notified, the task can be removed
    }
}