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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * This service is used by persistent tasks and allocated persistent tasks to communicate changes
 * to the master node so that the master can update the cluster state and can track of the states
 * of the persistent tasks.
 */
public class PersistentTasksService {

    private static final Logger logger = LogManager.getLogger(PersistentTasksService.class);

    public static final String PERSISTENT_TASK_ORIGIN = "persistent_tasks";

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public PersistentTasksService(ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.client = new OriginSettingClient(client, PERSISTENT_TASK_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Notifies the master node to create new persistent task and to assign it to a node. Accepts operation timeout as optional parameter
     */
    public <Params extends PersistentTaskParams> void sendStartRequest(
        final String taskId,
        final String taskName,
        final Params taskParams,
        final TimeValue timeout,
        final ActionListener<PersistentTask<Params>> listener
    ) {
        @SuppressWarnings("unchecked")
        final ActionListener<PersistentTask<?>> wrappedListener = listener.map(t -> (PersistentTask<Params>) t);
        execute(
            new StartPersistentTaskAction.Request(Objects.requireNonNull(timeout), taskId, taskName, taskParams),
            StartPersistentTaskAction.INSTANCE,
            wrappedListener
        );
    }

    /**
     * Notifies the master node about the completion of a persistent task.
     * <p>
     * At most one of {@code failure} and {@code localAbortReason} may be
     * provided. When both {@code failure} and {@code localAbortReason} are
     * {@code null}, the persistent task is considered as successfully completed.
     * Accepts operation timeout as optional parameter
     */
    public void sendCompletionRequest(
        final String taskId,
        final long taskAllocationId,
        final @Nullable Exception taskFailure,
        final @Nullable String localAbortReason,
        final @Nullable TimeValue timeout,
        final ActionListener<PersistentTask<?>> listener
    ) {
        execute(
            new CompletionPersistentTaskAction.Request(
                Objects.requireNonNull(timeout),
                taskId,
                taskAllocationId,
                taskFailure,
                localAbortReason
            ),
            CompletionPersistentTaskAction.INSTANCE,
            listener
        );
    }

    /**
     * Cancels a locally running task using the Task Manager API. Accepts operation timeout as optional parameter
     */
    void sendCancelRequest(final long taskId, final String reason, final ActionListener<ListTasksResponse> listener) {
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(clusterService.localNode().getId(), taskId));
        request.setReason(reason);
        // TODO set timeout?
        try {
            client.admin().cluster().cancelTasks(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Notifies the master node that the state of a persistent task has changed.
     * <p>
     * Persistent task implementers shouldn't call this method directly and use
     * {@link AllocatedPersistentTask#updatePersistentTaskState} instead.
     * Accepts operation timeout as optional parameter
     */
    void sendUpdateStateRequest(
        final String taskId,
        final long taskAllocationID,
        final PersistentTaskState taskState,
        final TimeValue timeout,
        final ActionListener<PersistentTask<?>> listener
    ) {
        execute(
            new UpdatePersistentTaskStatusAction.Request(Objects.requireNonNull(timeout), taskId, taskAllocationID, taskState),
            UpdatePersistentTaskStatusAction.INSTANCE,
            listener
        );
    }

    /**
     * Notifies the master node to remove a persistent task from the cluster state. Accepts operation timeout as optional parameter
     */
    public void sendRemoveRequest(final String taskId, final TimeValue timeout, final ActionListener<PersistentTask<?>> listener) {
        execute(
            new RemovePersistentTaskAction.Request(Objects.requireNonNull(timeout), taskId),
            RemovePersistentTaskAction.INSTANCE,
            listener
        );
    }

    /**
     * Executes an asynchronous persistent task action using the client.
     * <p>
     * The origin is set in the context and the listener is wrapped to ensure the proper context is restored
     */
    private <Req extends ActionRequest, Resp extends PersistentTaskResponse> void execute(
        final Req request,
        final ActionType<Resp> action,
        final ActionListener<PersistentTask<?>> listener
    ) {
        try {
            client.execute(action, request, listener.map(PersistentTaskResponse::getTask));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Waits for a given persistent task to comply with a given predicate, then call back the listener accordingly.
     *
     * @param taskId the persistent task id
     * @param predicate the persistent task predicate to evaluate
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    @Deprecated(forRemoval = true)
    public void waitForPersistentTaskCondition(
        final String taskId,
        final Predicate<PersistentTask<?>> predicate,
        final @Nullable TimeValue timeout,
        final WaitForPersistentTaskListener<?> listener
    ) {
        final var projectId = clusterService.state().metadata().getProject().id();
        waitForPersistentTaskCondition(projectId, taskId, predicate, timeout, listener);
    }

    /**
     * Waits for a given persistent task to comply with a given predicate, then call back the listener accordingly.
     *
     * @param projectId the project ID
     * @param taskId the persistent task id
     * @param predicate the persistent task predicate to evaluate, must be able to handle {@code null} input which means either the project
     *                  does not exist or persistent tasks for the project do not exist
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTaskCondition(
        final ProjectId projectId,
        final String taskId,
        final Predicate<PersistentTask<?>> predicate,
        final @Nullable TimeValue timeout,
        final WaitForPersistentTaskListener<?> listener
    ) {
        ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                final var project = state.metadata().projects().get(projectId);
                listener.onResponse(project == null ? null : PersistentTasksCustomMetadata.getTaskWithId(project, taskId));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onTimeout(timeout);
            }
        }, clusterState -> {
            final var project = clusterState.metadata().projects().get(projectId);
            if (project == null) {
                logger.debug("project [{}] not found while waiting for persistent task [{}] to pass predicate", projectId, taskId);
                return predicate.test(null);
            } else {
                return predicate.test(PersistentTasksCustomMetadata.getTaskWithId(project, taskId));
            }
        }, timeout, logger);
    }

    // visible for testing
    ClusterService getClusterService() {
        return clusterService;
    }

    // visible for testing
    ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Waits for persistent tasks to comply with a given predicate, then call back the listener accordingly.
     *
     * @param projectId the project that the persistent tasks are associated with
     * @param predicate the predicate to evaluate, must be able to handle {@code null} input which means either the project
     *                  does not exist or persistent tasks for the project do not exist
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTasksCondition(
        final ProjectId projectId,
        final Predicate<PersistentTasksCustomMetadata> predicate,
        final @Nullable TimeValue timeout,
        final ActionListener<Boolean> listener
    ) {
        ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                listener.onResponse(true);
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onFailure(new IllegalStateException("Timed out when waiting for persistent tasks after " + timeout));
            }
        }, clusterState -> {
            final var project = clusterState.metadata().projects().get(projectId);
            if (project == null) {
                logger.debug("project [{}] not found while waiting for persistent tasks condition", projectId);
                return predicate.test(null);
            } else {
                return predicate.test(PersistentTasksCustomMetadata.get(project));
            }
        }, timeout, logger);
    }

    public interface WaitForPersistentTaskListener<P extends PersistentTaskParams> extends ActionListener<PersistentTask<P>> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("Timed out when waiting for persistent task after " + timeout));
        }
    }
}
