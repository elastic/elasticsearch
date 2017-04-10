/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * This component is responsible for coordination of execution of persistent tasks on individual nodes. It runs on all
 * non-transport client nodes in the cluster and monitors cluster state changes to detect started commands.
 */
public class PersistentTasksNodeService extends AbstractComponent implements ClusterStateListener {
    private final Map<PersistentTaskId, AllocatedPersistentTask> runningTasks = new HashMap<>();
    private final PersistentTasksService persistentTasksService;
    private final PersistentTasksExecutorRegistry persistentTasksExecutorRegistry;
    private final TaskManager taskManager;
    private final NodePersistentTasksExecutor nodePersistentTasksExecutor;


    public PersistentTasksNodeService(Settings settings,
                                      PersistentTasksService persistentTasksService,
                                      PersistentTasksExecutorRegistry persistentTasksExecutorRegistry,
                                      TaskManager taskManager, NodePersistentTasksExecutor nodePersistentTasksExecutor) {
        super(settings);
        this.persistentTasksService = persistentTasksService;
        this.persistentTasksExecutorRegistry = persistentTasksExecutorRegistry;
        this.taskManager = taskManager;
        this.nodePersistentTasksExecutor = nodePersistentTasksExecutor;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        PersistentTasksCustomMetaData tasks = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData previousTasks = event.previousState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        // Cluster State   Local State      Local Action
        //   STARTED         NULL          Create as STARTED, Start
        //   STARTED         STARTED       Noop - running
        //   STARTED         COMPLETED     Noop - waiting for notification ack

        //   NULL            NULL          Noop - nothing to do
        //   NULL            STARTED       Remove locally, Mark as CANCELLED, Cancel
        //   NULL            COMPLETED     Remove locally

        // Master states:
        // NULL - doesn't exist in the cluster state
        // STARTED - exist in the cluster state

        // Local state:
        // NULL - we don't have task registered locally in runningTasks
        // STARTED - registered in TaskManager, requires master notification when finishes
        // CANCELLED - registered in TaskManager, doesn't require master notification when finishes
        // COMPLETED - not registered in TaskManager, notified, waiting for master to remove it from CS so we can remove locally

        // When task finishes if it is marked as STARTED or CANCELLED it is marked as COMPLETED and unregistered,
        // If the task was STARTED, the master notification is also triggered (this is handled by unregisterTask() method, which is
        // triggered by PersistentTaskListener

        if (Objects.equals(tasks, previousTasks) == false || event.nodesChanged()) {
            // We have some changes let's check if they are related to our node
            String localNodeId = event.state().getNodes().getLocalNodeId();
            Set<PersistentTaskId> notVisitedTasks = new HashSet<>(runningTasks.keySet());
            if (tasks != null) {
                for (PersistentTask<?> taskInProgress : tasks.tasks()) {
                    if (localNodeId.equals(taskInProgress.getExecutorNode())) {
                        PersistentTaskId persistentTaskId = new PersistentTaskId(taskInProgress.getId(), taskInProgress.getAllocationId());
                        AllocatedPersistentTask persistentTask = runningTasks.get(persistentTaskId);
                        if (persistentTask == null) {
                            // New task - let's start it
                            startTask(taskInProgress);
                        } else {
                            // The task is still running
                            notVisitedTasks.remove(persistentTaskId);
                        }
                    }
                }
            }

            for (PersistentTaskId id : notVisitedTasks) {
                AllocatedPersistentTask task = runningTasks.get(id);
                if (task.getState() == AllocatedPersistentTask.State.COMPLETED) {
                    // Result was sent to the caller and the caller acknowledged acceptance of the result
                    finishTask(id);
                } else {
                    // task is running locally, but master doesn't know about it - that means that the persistent task was removed
                    // cancel the task without notifying master
                    logger.trace("Found unregistered persistent task with id {} - cancelling ", id);
                    cancelTask(id);
                }
            }

        }

    }

    private <Request extends PersistentTaskRequest> void startTask(PersistentTask<Request> taskInProgress) {
        PersistentTasksExecutor<Request> action = persistentTasksExecutorRegistry.getPersistentTaskExecutorSafe(taskInProgress.getTaskName());
        AllocatedPersistentTask task = (AllocatedPersistentTask) taskManager.register("persistent", taskInProgress.getTaskName() + "[c]",
                taskInProgress.getRequest());
        boolean processed = false;
        try {
            task.init(persistentTasksService, taskInProgress.getId(), taskInProgress.getAllocationId());
            PersistentTaskListener listener = new PersistentTaskListener(task);
            try {
                runningTasks.put(new PersistentTaskId(taskInProgress.getId(), taskInProgress.getAllocationId()), task);
                nodePersistentTasksExecutor.executeTask(taskInProgress.getRequest(), task, action, listener);
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

    /**
     * Unregisters the locally running task. No notification to master will be send upon cancellation.
     */
    private void finishTask(PersistentTaskId persistentTaskId) {
        AllocatedPersistentTask task = runningTasks.remove(persistentTaskId);
        if (task != null) {
            taskManager.unregister(task);
        }
    }

    /**
     * Unregisters and then cancels the locally running task using the task manager. No notification to master will be send upon
     * cancellation.
     */
    private void cancelTask(PersistentTaskId persistentTaskId) {
        AllocatedPersistentTask task = runningTasks.remove(persistentTaskId);
        if (task != null) {
            if (task.markAsCancelled()) {
                // Cancel the local task using the task manager
                persistentTasksService.sendTaskManagerCancellation(task.getId(), new ActionListener<CancelTasksResponse>() {
                    @Override
                    public void onResponse(CancelTasksResponse cancelTasksResponse) {
                        logger.trace("Persistent task with id {} was cancelled", task.getId());

                    }

                    @Override
                    public void onFailure(Exception e) {
                        // There is really nothing we can do in case of failure here
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to cancel task {}", task.getPersistentTaskId()), e);
                    }
                });
            }
        }
    }

    private void unregisterTask(AllocatedPersistentTask task, Exception e) {
        AllocatedPersistentTask.State prevState = task.markAsCompleted(e);
        if (prevState == AllocatedPersistentTask.State.CANCELLED) {
            // The task was cancelled by master - no need to send notifications
            taskManager.unregister(task);
        } else if (prevState == AllocatedPersistentTask.State.STARTED) {
            // The task finished locally, but master doesn't know about it - we need notify the master before we can unregister it
            logger.trace("sending notification for completed task {}", task.getPersistentTaskId());
            persistentTasksService.sendCompletionNotification(task.getPersistentTaskId(), e, new ActionListener<PersistentTask<?>>() {
                @Override
                public void onResponse(PersistentTask<?> persistentTask) {
                    logger.trace("notification for task {} was successful", task.getId());
                    taskManager.unregister(task);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn((Supplier<?>) () ->
                            new ParameterizedMessage("notification for task {} failed", task.getPersistentTaskId()), e);
                    taskManager.unregister(task);
                }
            });
        } else {
            logger.warn("attempt to complete task {} in the {} state", task.getPersistentTaskId(), prevState);
        }
    }

    private class PersistentTaskListener implements ActionListener<Empty> {
        private final AllocatedPersistentTask task;

        PersistentTaskListener(final AllocatedPersistentTask task) {
            this.task = task;
        }

        @Override
        public void onResponse(Empty response) {
            unregisterTask(task, null);
        }

        @Override
        public void onFailure(Exception e) {
            if (task.isCancelled()) {
                // The task was explicitly cancelled - no need to restart it, just log the exception if it's not TaskCancelledException
                if (e instanceof TaskCancelledException == false) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage(
                            "cancelled task {} failed with an exception, cancellation reason [{}]",
                            task.getPersistentTaskId(), task.getReasonCancelled()), e);
                }
                if (CancelTasksRequest.DEFAULT_REASON.equals(task.getReasonCancelled())) {
                    unregisterTask(task, null);
                } else {
                    unregisterTask(task, e);
                }
            } else {
                unregisterTask(task, e);
            }
        }
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

    public static class Status implements Task.Status {
        public static final String NAME = "persistent_executor";

        private final AllocatedPersistentTask.State state;

        public Status(AllocatedPersistentTask.State state) {
            this.state = requireNonNull(state, "State cannot be null");
        }

        public Status(StreamInput in) throws IOException {
            state = AllocatedPersistentTask.State.valueOf(in.readString());
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

        public AllocatedPersistentTask.State getState() {
            return state;
        }

        @Override
        public boolean isFragment() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return state == status.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state);
        }
    }

}