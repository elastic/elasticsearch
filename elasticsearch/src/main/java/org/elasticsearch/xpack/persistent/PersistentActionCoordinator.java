/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

/**
 * This component is responsible for coordination of execution of persistent actions on individual nodes. It runs on all
 * non-transport client nodes in the cluster and monitors cluster state changes to detect started commands.
 */
public class PersistentActionCoordinator extends AbstractComponent implements ClusterStateListener {
    private final Map<PersistentTaskId, RunningPersistentTask> runningTasks = new HashMap<>();
    private final PersistentActionService persistentActionService;
    private final PersistentActionRegistry persistentActionRegistry;
    private final TaskManager taskManager;
    private final PersistentActionExecutor persistentActionExecutor;


    public PersistentActionCoordinator(Settings settings,
                                       PersistentActionService persistentActionService,
                                       PersistentActionRegistry persistentActionRegistry,
                                       TaskManager taskManager,
                                       PersistentActionExecutor persistentActionExecutor) {
        super(settings);
        this.persistentActionService = persistentActionService;
        this.persistentActionRegistry = persistentActionRegistry;
        this.taskManager = taskManager;
        this.persistentActionExecutor = persistentActionExecutor;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        PersistentTasksInProgress tasks = event.state().custom(PersistentTasksInProgress.TYPE);
        PersistentTasksInProgress previousTasks = event.previousState().custom(PersistentTasksInProgress.TYPE);

        if (Objects.equals(tasks, previousTasks) == false || event.nodesChanged()) {
            // We have some changes let's check if they are related to our node
            String localNodeId = event.state().getNodes().getLocalNodeId();
            Set<PersistentTaskId> notVisitedTasks = new HashSet<>(runningTasks.keySet());
            if (tasks != null) {
                for (PersistentTaskInProgress<?> taskInProgress : tasks.tasks()) {
                    if (localNodeId.equals(taskInProgress.getExecutorNode())) {
                        PersistentTaskId persistentTaskId = new PersistentTaskId(taskInProgress.getId(), taskInProgress.getAllocationId());
                        RunningPersistentTask persistentTask = runningTasks.get(persistentTaskId);
                        if (persistentTask == null) {
                            // New task - let's start it
                            startTask(taskInProgress);
                        } else {
                            // The task is still running
                            notVisitedTasks.remove(persistentTaskId);
                            if (persistentTask.getState() == State.FAILED_NOTIFICATION) {
                                // We tried to notify the master about this task before but the notification failed and
                                // the master doesn't seem to know about it - retry notification
                                restartCompletionNotification(persistentTask);
                            }
                        }
                    }
                }
            }

            for (PersistentTaskId id : notVisitedTasks) {
                RunningPersistentTask task = runningTasks.get(id);
                if (task.getState() == State.NOTIFIED || task.getState() == State.FAILED) {
                    // Result was sent to the caller and the caller acknowledged acceptance of the result
                    finishTask(id);
                } else if (task.getState() == State.FAILED_NOTIFICATION) {
                    // We tried to send result to master, but it failed and master doesn't know about this task
                    // this shouldn't really happen, unless this node is severally out of sync with the master
                    logger.warn("failed to notify master about task {}", task.getId());
                    finishTask(id);
                } else {
                    // task is running locally, but master doesn't know about it - that means that the persistent task was removed
                    // cancel the task without notifying master
                    cancelTask(id);
                }
            }

        }

    }

    private <Request extends PersistentActionRequest> void startTask(PersistentTaskInProgress<Request> taskInProgress) {
        PersistentActionRegistry.PersistentActionHolder<Request> holder =
                persistentActionRegistry.getPersistentActionHolderSafe(taskInProgress.getAction());
        PersistentTask task = (PersistentTask) taskManager.register("persistent", taskInProgress.getAction() + "[c]",
                taskInProgress.getRequest());
        boolean processed = false;
        try {
            RunningPersistentTask runningPersistentTask = new RunningPersistentTask(task, taskInProgress.getId());
            task.setStatusProvider(runningPersistentTask);
            task.setPersistentTaskId(taskInProgress.getId());
            PersistentTaskListener listener = new PersistentTaskListener(runningPersistentTask);
            try {
                runningTasks.put(new PersistentTaskId(taskInProgress.getId(), taskInProgress.getAllocationId()), runningPersistentTask);
                persistentActionExecutor.executeAction(taskInProgress.getRequest(), task, holder, listener);
            } catch (Exception e) {
                // Submit task failure
                listener.onFailure(e);
            }
            processed = true;
        } finally {
            if (processed == false) {
                // something went wrong - unregistering task
                taskManager.unregister(task);
            }
        }
    }

    private void finishTask(PersistentTaskId persistentTaskId) {
        RunningPersistentTask task = runningTasks.remove(persistentTaskId);
        if (task != null && task.getTask() != null) {
            taskManager.unregister(task.getTask());
        }
    }

    private void cancelTask(PersistentTaskId persistentTaskId) {
        RunningPersistentTask task = runningTasks.remove(persistentTaskId);
        if (task != null && task.getTask() != null) {
            if (task.markAsCancelled()) {
                persistentActionService.sendCancellation(task.getTask().getId(), new ActionListener<CancelTasksResponse>() {
                    @Override
                    public void onResponse(CancelTasksResponse cancelTasksResponse) {
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // There is really nothing we can do in case of failure here
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to cancel task {}", task.getId()), e);
                    }
                });
            }
        }
    }


    private void restartCompletionNotification(RunningPersistentTask task) {
        logger.trace("resending notification for task {}", task.getId());
        if (task.getState() == State.CANCELLED) {
            taskManager.unregister(task.getTask());
        } else {
            if (task.restartCompletionNotification()) {
                persistentActionService.sendCompletionNotification(task.getId(), task.getFailure(), new PublishedResponseListener(task));
            } else {
                logger.warn("attempt to resend notification for task {} in the {} state", task.getId(), task.getState());
            }
        }
    }

    private void startCompletionNotification(RunningPersistentTask task, Exception e) {
        if (task.getState() == State.CANCELLED) {
            taskManager.unregister(task.getTask());
        } else {
            logger.trace("sending notification for failed task {}", task.getId());
            if (task.startNotification(e)) {
                persistentActionService.sendCompletionNotification(task.getId(), e, new PublishedResponseListener(task));
            } else {
                logger.warn("attempt to send notification for task {} in the {} state", task.getId(), task.getState());
            }
        }
    }

    private class PersistentTaskListener implements ActionListener<Empty> {
        private final RunningPersistentTask task;

        PersistentTaskListener(final RunningPersistentTask task) {
            this.task = task;
        }

        @Override
        public void onResponse(Empty response) {
            startCompletionNotification(task, null);
        }

        @Override
        public void onFailure(Exception e) {
            if (task.getTask().isCancelled()) {
                // The task was explicitly cancelled - no need to restart it, just log the exception if it's not TaskCancelledException
                if (e instanceof TaskCancelledException == false) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "cancelled task {} failed with an exception, cancellation reason [{}]",
                            task.getId(), task.getTask().getReasonCancelled()), e);
                }
                startCompletionNotification(task, null);
            } else {
                startCompletionNotification(task, e);
            }
        }
    }

    private class PublishedResponseListener implements ActionListener<CompletionPersistentTaskAction.Response> {
        private final RunningPersistentTask task;

        PublishedResponseListener(final RunningPersistentTask task) {
            this.task = task;
        }


        @Override
        public void onResponse(CompletionPersistentTaskAction.Response response) {
            logger.trace("notification for task {} was successful", task.getId());
            if (task.markAsNotified() == false) {
                logger.warn("attempt to mark task {} in the {} state as NOTIFIED", task.getId(), task.getState());
            }
            taskManager.unregister(task.getTask());
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("notification for task {} failed - retrying", task.getId()), e);
            if (task.notificationFailed() == false) {
                logger.warn("attempt to mark restart notification for task {} in the {} state failed", task.getId(), task.getState());
            }
        }
    }

    public enum State {
        STARTED,  // the task is currently running
        CANCELLED, // the task is cancelled
        FAILED,     // the task is done running and trying to notify caller
        FAILED_NOTIFICATION, // the caller notification failed
        NOTIFIED // the caller was notified, the task can be removed
    }

    private static class PersistentTaskId {
        private final long id;
        private final long allocationId;

        PersistentTaskId(long id, long allocationId) {
            this.id = id;
            this.allocationId = allocationId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PersistentTaskId that = (PersistentTaskId) o;
            return id == that.id &&
                    allocationId == that.allocationId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, allocationId);
        }
    }

    private static class RunningPersistentTask implements Provider<Task.Status> {
        private final PersistentTask task;
        private final long id;
        private final AtomicReference<State> state;
        @Nullable
        private Exception failure;

        RunningPersistentTask(PersistentTask task, long id) {
            this(task, id, State.STARTED);
        }

        RunningPersistentTask(PersistentTask task, long id, State state) {
            this.task = task;
            this.id = id;
            this.state = new AtomicReference<>(state);
        }

        public PersistentTask getTask() {
            return task;
        }

        public long getId() {
            return id;
        }

        public State getState() {
            return state.get();
        }

        public Exception getFailure() {
            return failure;
        }

        public boolean startNotification(Exception failure) {
            boolean result = state.compareAndSet(State.STARTED, State.FAILED);
            if (result) {
                this.failure = failure;
            }
            return result;
        }

        public boolean notificationFailed() {
            return state.compareAndSet(State.FAILED, State.FAILED_NOTIFICATION);
        }

        public boolean restartCompletionNotification() {
            return state.compareAndSet(State.FAILED_NOTIFICATION, State.FAILED);
        }

        public boolean markAsNotified() {
            return state.compareAndSet(State.FAILED, State.NOTIFIED);
        }

        public boolean markAsCancelled() {
            return state.compareAndSet(State.STARTED, State.CANCELLED);
        }

        @Override
        public Task.Status get() {
            return new Status(state.get());
        }
    }

    public static class Status implements Task.Status {
        public static final String NAME = "persistent_executor";

        private final State state;

        public Status(State state) {
            this.state = requireNonNull(state, "State cannot be null");
        }

        public Status(StreamInput in) throws IOException {
            state = State.valueOf(in.readString());
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("state", state.toString());
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(state.toString());
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

        public State getState() {
            return state;
        }

        @Override
        public boolean isFragment() {
            return false;
        }
    }

}