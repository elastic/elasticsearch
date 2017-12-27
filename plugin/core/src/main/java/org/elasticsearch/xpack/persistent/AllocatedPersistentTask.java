/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a executor node operation that corresponds to a persistent task
 */
public class AllocatedPersistentTask extends CancellableTask {
    private volatile String persistentTaskId;
    private volatile long allocationId;

    private final AtomicReference<State> state;
    @Nullable
    private volatile Exception failure;

    private volatile PersistentTasksService persistentTasksService;
    private volatile Logger logger;
    private volatile TaskManager taskManager;


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
     * <p>
     * This doesn't affect the status of this allocated task.
     */
    public void updatePersistentStatus(Task.Status status, ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>> listener) {
        persistentTasksService.updateStatus(persistentTaskId, allocationId, status, listener);
    }

    public String getPersistentTaskId() {
        return persistentTaskId;
    }

    void init(PersistentTasksService persistentTasksService, TaskManager taskManager, Logger logger, String persistentTaskId, long
            allocationId) {
        this.persistentTasksService = persistentTasksService;
        this.logger = logger;
        this.taskManager = taskManager;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
    }

    public Exception getFailure() {
        return failure;
    }

    boolean markAsCancelled() {
        return state.compareAndSet(AllocatedPersistentTask.State.STARTED, AllocatedPersistentTask.State.PENDING_CANCEL);
    }

    public State getState() {
        return state.get();
    }

    public long getAllocationId() {
        return allocationId;
    }

    public enum State {
        STARTED,  // the task is currently running
        PENDING_CANCEL, // the task is cancelled on master, cancelling it locally
        COMPLETED     // the task is done running and trying to notify caller
    }

    public void markAsCompleted() {
        completeAndNotifyIfNeeded(null);
    }

    public void markAsFailed(Exception e) {
        if (CancelTasksRequest.DEFAULT_REASON.equals(getReasonCancelled())) {
            completeAndNotifyIfNeeded(null);
        } else {
            completeAndNotifyIfNeeded(e);
        }

    }

    private void completeAndNotifyIfNeeded(@Nullable Exception failure) {
        State prevState = state.getAndSet(AllocatedPersistentTask.State.COMPLETED);
        if (prevState == State.COMPLETED) {
            logger.warn("attempt to complete task [{}] with id [{}] in the [{}] state", getAction(), getPersistentTaskId(), prevState);
        } else {
            if (failure != null) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                        "task {} failed with an exception", getPersistentTaskId()), failure);
            }
            try {
                this.failure = failure;
                if (prevState == State.STARTED) {
                    logger.trace("sending notification for completed task [{}] with id [{}]", getAction(), getPersistentTaskId());
                    persistentTasksService.sendCompletionNotification(getPersistentTaskId(), getAllocationId(), failure, new
                            ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                                @Override
                                public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                                    logger.trace("notification for task [{}] with id [{}] was successful", getAction(),
                                            getPersistentTaskId());
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn((Supplier<?>) () ->
                                            new ParameterizedMessage("notification for task [{}] with id [{}] failed",
                                                    getAction(), getPersistentTaskId()), e);
                                }
                            });
                }
            } finally {
                taskManager.unregister(this);
            }
        }
    }
}
