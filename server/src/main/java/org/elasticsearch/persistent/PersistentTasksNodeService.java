/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.core.Strings.format;

/**
 * This component is responsible for coordination of execution of persistent tasks on individual nodes. It runs on all
 * nodes in the cluster and monitors cluster state changes to detect started commands.
 */
public class PersistentTasksNodeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PersistentTasksNodeService.class);

    private final ThreadPool threadPool;
    private final Map<Long, AllocatedPersistentTask> runningTasks = new HashMap<>();
    private final PersistentTasksService persistentTasksService;
    private final PersistentTasksExecutorRegistry persistentTasksExecutorRegistry;
    private final TaskManager taskManager;
    private final NodePersistentTasksExecutor nodePersistentTasksExecutor;

    public PersistentTasksNodeService(
        ThreadPool threadPool,
        PersistentTasksService persistentTasksService,
        PersistentTasksExecutorRegistry persistentTasksExecutorRegistry,
        TaskManager taskManager,
        NodePersistentTasksExecutor nodePersistentTasksExecutor
    ) {
        this.threadPool = threadPool;
        this.persistentTasksService = persistentTasksService;
        this.persistentTasksExecutorRegistry = persistentTasksExecutorRegistry;
        this.taskManager = taskManager;
        this.nodePersistentTasksExecutor = nodePersistentTasksExecutor;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise if the only master restarts
            // we start cancelling all local tasks before cluster has a chance to recover.
            return;
        }
        PersistentTasksCustomMetadata tasks = event.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata previousTasks = event.previousState().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);

        /*
         * Master states:
         * NULL    - doesn't exist in the cluster state
         * STARTED - exist in the cluster state
         *
         * Local states (see org.elasticsearch.persistent.AllocatedPersistentTask.State)
         * NULL           - we don't have task registered locally in runningTasks
         * STARTED        - registered in TaskManager, requires master notification when finishes
         * PENDING_CANCEL - registered in TaskManager, doesn't require master notification when finishes
         * COMPLETED      - not registered in TaskManager, notified, waiting for master to remove it from CS so we can remove locally
         * LOCAL_ABORTED  - not registered in TaskManager, notified, waiting for master to adjust it in CS so we can remove locally
         *
         *  Master state  | Local state    | Local action
         * ---------------+----------------+-----------------------------------------------
         *  STARTED       | NULL           | Create as STARTED, Start
         *  STARTED       | STARTED        | Noop - running
         *  STARTED       | PENDING_CANCEL | Impossible
         *  STARTED       | COMPLETED      | Noop - waiting for notification ack
         *  STARTED       | LOCAL_ABORTED  | Noop - waiting for notification ack
         *  NULL          | NULL           | Noop - nothing to do
         *  NULL          | STARTED        | Remove locally, Mark as PENDING_CANCEL, Cancel
         *  NULL          | PENDING_CANCEL | Noop - will remove locally when complete
         *  NULL          | COMPLETED      | Remove locally
         *  NULL          | LOCAL_ABORTED  | Remove locally
         *
         * When task finishes if it is marked as STARTED or PENDING_CANCEL it is marked as COMPLETED and unregistered,
         * If the task was STARTED, the master notification is also triggered (this is handled by unregisterTask() method, which is
         * triggered by PersistentTaskListener
         */

        if (Objects.equals(tasks, previousTasks) == false || event.nodesChanged()) {
            // We have some changes let's check if they are related to our node
            String localNodeId = event.state().getNodes().getLocalNodeId();
            Set<Long> notVisitedTasks = new HashSet<>(runningTasks.keySet());
            if (tasks != null) {
                for (PersistentTask<?> taskInProgress : tasks.tasks()) {
                    if (localNodeId.equals(taskInProgress.getExecutorNode())) {
                        Long allocationId = taskInProgress.getAllocationId();
                        AllocatedPersistentTask persistentTask = runningTasks.get(allocationId);
                        if (persistentTask == null) {
                            // New task - let's start it
                            try {
                                startTask(taskInProgress);
                            } catch (Exception e) {
                                logger.error(
                                    "Unable to start allocated task ["
                                        + taskInProgress.getTaskName()
                                        + "] with id ["
                                        + taskInProgress.getId()
                                        + "] and allocation id ["
                                        + taskInProgress.getAllocationId()
                                        + "]",
                                    e
                                );
                            }
                        } else {
                            // The task is still running
                            notVisitedTasks.remove(allocationId);
                        }
                    }
                }
            }

            for (Long id : notVisitedTasks) {
                AllocatedPersistentTask task = runningTasks.get(id);
                if (task.isCompleted()) {
                    // Result was sent to the caller and the caller acknowledged acceptance of the result
                    logger.trace(
                        "Found persistent task [{}] with id [{}], allocation id [{}] and status [{}] - removing",
                        task.getAction(),
                        task.getPersistentTaskId(),
                        task.getAllocationId(),
                        task.getStatus()
                    );
                    runningTasks.remove(id);
                } else {
                    // task is running locally, but master doesn't know about it - that means that the persistent task was removed
                    // cancel the task without notifying master
                    logger.trace(
                        "Found unregistered persistent task [{}] with id [{}] and allocation id [{}] - cancelling",
                        task.getAction(),
                        task.getPersistentTaskId(),
                        task.getAllocationId()
                    );
                    cancelTask(id);
                }
            }

        }

    }

    private <Params extends PersistentTaskParams> void startTask(PersistentTask<Params> taskInProgress) {
        PersistentTasksExecutor<Params> executor = persistentTasksExecutorRegistry.getPersistentTaskExecutorSafe(
            taskInProgress.getTaskName()
        );

        final var request = new PersistentTaskAwareRequest<>(taskInProgress, executor);
        try (var ignored = threadPool.getThreadContext().newTraceContext()) {
            doStartTask(taskInProgress, executor, request);
        }
    }

    /**
     * A {@link TaskAwareRequest} which creates the relevant task using a {@link PersistentTasksExecutor}.
     */
    private static class PersistentTaskAwareRequest<Params extends PersistentTaskParams> implements TaskAwareRequest {
        private final PersistentTask<Params> taskInProgress;
        private final TaskId parentTaskId;
        private final PersistentTasksExecutor<Params> executor;

        private PersistentTaskAwareRequest(PersistentTask<Params> taskInProgress, PersistentTasksExecutor<Params> executor) {
            this.taskInProgress = taskInProgress;
            this.parentTaskId = new TaskId("cluster", taskInProgress.getAllocationId());
            this.executor = executor;
        }

        @Override
        public void setParentTask(TaskId taskId) {
            throw new UnsupportedOperationException("parent task if for persistent tasks shouldn't change");
        }

        @Override
        public void setRequestId(long requestId) {
            throw new UnsupportedOperationException("does not have a request ID");
        }

        @Override
        public TaskId getParentTask() {
            return parentTaskId;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return executor.createTask(id, type, action, parentTaskId, taskInProgress, headers);
        }
    }

    /**
     * A no-op {@link PersistentTasksExecutor} to create a placeholder task if creating the real task fails for some reason.
     */
    private static class PersistentTaskStartupFailureExecutor<Params extends PersistentTaskParams> extends PersistentTasksExecutor<Params> {
        PersistentTaskStartupFailureExecutor(String taskName, Executor executor) {
            super(taskName, executor);
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, Params params, PersistentTaskState state) {}
    }

    private <Params extends PersistentTaskParams> void doStartTask(
        PersistentTask<Params> taskInProgress,
        PersistentTasksExecutor<Params> executor,
        TaskAwareRequest request
    ) {
        final AllocatedPersistentTask task;
        try {
            task = (AllocatedPersistentTask) taskManager.register("persistent", taskInProgress.getTaskName() + "[c]", request);
        } catch (Exception e) {
            logger.error(
                "Fatal error registering persistent task ["
                    + taskInProgress.getTaskName()
                    + "] with id ["
                    + taskInProgress.getId()
                    + "] and allocation id ["
                    + taskInProgress.getAllocationId()
                    + "], removing from persistent tasks",
                e
            );

            // create a no-op placeholder task so that we don't keep trying to start this task while we wait for the cluster state update
            // which handles the failure
            final var placeholderTask = (AllocatedPersistentTask) taskManager.register(
                "persistent",
                taskInProgress.getTaskName() + "[c]",
                new PersistentTaskAwareRequest<>(
                    taskInProgress,
                    new PersistentTaskStartupFailureExecutor<>(executor.getTaskName(), EsExecutors.DIRECT_EXECUTOR_SERVICE)
                )
            );
            placeholderTask.init(persistentTasksService, taskManager, taskInProgress.getId(), taskInProgress.getAllocationId());
            taskManager.unregister(placeholderTask);
            runningTasks.put(taskInProgress.getAllocationId(), placeholderTask);
            placeholderTask.markAsFailed(e);
            return;
        }

        boolean processed = false;
        Exception initializationException = null;
        try {
            task.init(persistentTasksService, taskManager, taskInProgress.getId(), taskInProgress.getAllocationId());
            logger.trace(
                "Persistent task [{}] with id [{}] and allocation id [{}] was created",
                task.getAction(),
                task.getPersistentTaskId(),
                task.getAllocationId()
            );
            try {
                runningTasks.put(taskInProgress.getAllocationId(), task);
                nodePersistentTasksExecutor.executeTask(taskInProgress.getParams(), taskInProgress.getState(), task, executor);
            } catch (Exception e) {
                // Submit task failure
                task.markAsFailed(e);
            }
            processed = true;
        } catch (Exception e) {
            initializationException = e;
        } finally {
            if (processed == false) {
                // something went wrong - unregistering task
                logger.warn(
                    "Persistent task [{}] with id [{}] and allocation id [{}] failed to create",
                    task.getAction(),
                    task.getPersistentTaskId(),
                    task.getAllocationId()
                );
                taskManager.unregister(task);
                if (initializationException != null) {
                    notifyMasterOfFailedTask(taskInProgress, initializationException);
                }
            }
        }
    }

    private <Params extends PersistentTaskParams> void notifyMasterOfFailedTask(
        PersistentTask<Params> taskInProgress,
        Exception originalException
    ) {
        persistentTasksService.sendCompletionRequest(
            taskInProgress.getId(),
            taskInProgress.getAllocationId(),
            originalException,
            null,
            null,
            new ActionListener<>() {
                @Override
                public void onResponse(PersistentTask<?> persistentTask) {
                    logger.trace(
                        "completion notification for failed task [{}] with id [{}] was successful",
                        taskInProgress.getTaskName(),
                        taskInProgress.getAllocationId()
                    );
                }

                @Override
                public void onFailure(Exception notificationException) {
                    notificationException.addSuppressed(originalException);
                    logger.warn(
                        () -> format(
                            "notification for task [%s] with id [%s] failed",
                            taskInProgress.getTaskName(),
                            taskInProgress.getAllocationId()
                        ),
                        notificationException
                    );
                }
            }
        );
    }

    /**
     * Unregisters and then cancels the locally running task using the task manager. No notification to master will be sent upon
     * cancellation.
     */
    private void cancelTask(Long allocationId) {
        AllocatedPersistentTask task = runningTasks.remove(allocationId);
        if (task.markAsCancelled()) {
            // Cancel the local task using the task manager
            String reason = "task has been removed, cancelling locally";
            persistentTasksService.sendCancelRequest(task.getId(), reason, null, new ActionListener<>() {
                @Override
                public void onResponse(ListTasksResponse cancelTasksResponse) {
                    logger.trace(
                        "Persistent task [{}] with id [{}] and allocation id [{}] was cancelled",
                        task.getAction(),
                        task.getPersistentTaskId(),
                        task.getAllocationId()
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    // There is really nothing we can do in case of failure here
                    logger.warn(
                        () -> format(
                            "failed to cancel task [%s] with id [%s] and allocation id [%s]",
                            task.getAction(),
                            task.getPersistentTaskId(),
                            task.getAllocationId()
                        ),
                        e
                    );
                }
            });
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
