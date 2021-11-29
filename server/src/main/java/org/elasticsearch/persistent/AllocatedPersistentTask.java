/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Represents a executor node operation that corresponds to a persistent task
 */
public class AllocatedPersistentTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(AllocatedPersistentTask.class);
    private final AtomicReference<State> state;

    private volatile String persistentTaskId;
    private volatile long allocationId;
    private volatile @Nullable Exception failure;
    private volatile PersistentTasksService persistentTasksService;
    private volatile TaskManager taskManager;

    public AllocatedPersistentTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
        this.state = new AtomicReference<>(State.STARTED);
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
    public void updatePersistentTaskState(
        final PersistentTaskState state,
        final ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
    ) {
        persistentTasksService.sendUpdateStateRequest(persistentTaskId, allocationId, state, listener);
    }

    public String getPersistentTaskId() {
        return persistentTaskId;
    }

    protected void init(
        PersistentTasksService persistentTasksService,
        TaskManager taskManager,
        String persistentTaskId,
        long allocationId
    ) {
        this.persistentTasksService = persistentTasksService;
        this.taskManager = taskManager;
        this.persistentTaskId = persistentTaskId;
        this.allocationId = allocationId;
    }

    public Exception getFailure() {
        return failure;
    }

    public long getAllocationId() {
        return allocationId;
    }

    /**
     * Waits for a given persistent task to comply with a given predicate, then call back the listener accordingly.
     *
     * @param predicate the persistent task predicate to evaluate
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTask(
        final Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> predicate,
        final @Nullable TimeValue timeout,
        final PersistentTasksService.WaitForPersistentTaskListener<?> listener
    ) {
        persistentTasksService.waitForPersistentTaskCondition(persistentTaskId, predicate, timeout, listener);
    }

    /**
     * For external purposes, locally aborted and completed are the same.
     * @return Is this task completed on the current node?
     */
    protected final boolean isCompleted() {
        return state.get() == State.COMPLETED || state.get() == State.LOCAL_ABORTED;
    }

    protected boolean markAsCancelled() {
        return state.compareAndSet(State.STARTED, State.PENDING_CANCEL);
    }

    public void markAsCompleted() {
        completeAndNotifyIfNeeded(null, null);
    }

    public void markAsFailed(Exception e) {
        if (CancelTasksRequest.DEFAULT_REASON.equals(getReasonCancelled())) {
            completeAndNotifyIfNeeded(null, null);
        } else {
            completeAndNotifyIfNeeded(e, null);
        }
    }

    /**
     * Indicates that this persistent task is no longer going to run on the local node.
     * This will cause the local task to be terminated, and the associated persistent
     * task to be reassigned by the master node. The persistent task <em>may</em> be
     * reassigned to the same node unless separate measures have been taken to prevent
     * this. The task should complete any graceful shutdown actions before calling this
     * method.
     * @param localAbortReason Reason for the task being aborted on this node. This
     *                         will be recorded as the reason for unassignment of the
     *                         persistent task.
     */
    public void markAsLocallyAborted(String localAbortReason) {
        completeAndNotifyIfNeeded(null, Objects.requireNonNull(localAbortReason));
    }

    private void completeAndNotifyIfNeeded(@Nullable Exception failure, @Nullable String localAbortReason) {
        assert failure == null || localAbortReason == null
            : "completion notification has both exception " + failure + " and local abort reason " + localAbortReason;
        final State desiredState = (localAbortReason == null) ? State.COMPLETED : State.LOCAL_ABORTED;
        final State prevState = state.getAndUpdate(
            currentState -> (currentState != State.COMPLETED && currentState != State.LOCAL_ABORTED) ? desiredState : currentState
        );
        if (prevState == State.COMPLETED || prevState == State.LOCAL_ABORTED) {
            // To preserve old behaviour completing a task twice is not an error.
            // However, any combination of local abort with completion or failure
            // is an error, as is issuing two local aborts for the same task.
            if (desiredState == State.COMPLETED) {
                if (prevState == State.COMPLETED) {
                    logger.warn(
                        "attempt to complete task [{}] with id [{}] in the [{}] state",
                        getAction(),
                        getPersistentTaskId(),
                        prevState
                    );
                } else {
                    throw new IllegalStateException(
                        "attempt to "
                            + (failure != null ? "fail" : "complete")
                            + " task ["
                            + getAction()
                            + "] with id ["
                            + getPersistentTaskId()
                            + "] which has been locally aborted"
                    );
                }
            } else {
                throw new IllegalStateException(
                    "attempt to locally abort task ["
                        + getAction()
                        + "] with id ["
                        + getPersistentTaskId()
                        + "] which has already been "
                        + (prevState == State.COMPLETED ? "completed" : "locally aborted")
                );
            }
        } else {
            if (failure != null) {
                logger.warn(() -> new ParameterizedMessage("task [{}] failed with an exception", getPersistentTaskId()), failure);
            } else if (localAbortReason != null) {
                logger.debug("task [{}] aborted locally: [{}]", getPersistentTaskId(), localAbortReason);
            }
            try {
                this.failure = failure;
                if (prevState == State.STARTED) {
                    logger.trace("sending notification for completed task [{}] with id [{}]", getAction(), getPersistentTaskId());
                    persistentTasksService.sendCompletionRequest(
                        getPersistentTaskId(),
                        getAllocationId(),
                        failure,
                        localAbortReason,
                        new ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
                                logger.trace("notification for task [{}] with id [{}] was successful", getAction(), getPersistentTaskId());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn(
                                    () -> new ParameterizedMessage(
                                        "notification for task [{}] with id [{}] failed",
                                        getAction(),
                                        getPersistentTaskId()
                                    ),
                                    e
                                );
                            }
                        }
                    );
                }
            } finally {
                taskManager.unregister(this);
            }
        }
    }

    public enum State {
        STARTED,        // the task is currently running
        PENDING_CANCEL, // the task is cancelled on master, cancelling it locally
        COMPLETED,      // the task is done running and trying to notify caller
        LOCAL_ABORTED   // the task is aborted on the local node - master should reassign
    }
}
