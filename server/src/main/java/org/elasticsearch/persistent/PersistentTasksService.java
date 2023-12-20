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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

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
     * Notifies the master node to create new persistent task and to assign it to a node.
     */
    public <Params extends PersistentTaskParams> void sendStartRequest(
        final String taskId,
        final String taskName,
        final Params taskParams,
        final ActionListener<PersistentTask<Params>> listener
    ) {
        @SuppressWarnings("unchecked")
        final ActionListener<PersistentTask<?>> wrappedListener = listener.map(t -> (PersistentTask<Params>) t);
        StartPersistentTaskAction.Request request = new StartPersistentTaskAction.Request(taskId, taskName, taskParams);
        execute(request, StartPersistentTaskAction.INSTANCE, wrappedListener);
    }

    /**
     * Notifies the master node about the completion of a persistent task.
     * <p>
     * At most one of {@code failure} and {@code localAbortReason} may be
     * provided. When both {@code failure} and {@code localAbortReason} are
     * {@code null}, the persistent task is considered as successfully completed.
     */
    public void sendCompletionRequest(
        final String taskId,
        final long taskAllocationId,
        final @Nullable Exception taskFailure,
        final @Nullable String localAbortReason,
        final ActionListener<PersistentTask<?>> listener
    ) {
        CompletionPersistentTaskAction.Request request = new CompletionPersistentTaskAction.Request(
            taskId,
            taskAllocationId,
            taskFailure,
            localAbortReason
        );
        execute(request, CompletionPersistentTaskAction.INSTANCE, listener);
    }

    /**
     * Cancels a locally running task using the Task Manager API
     */
    void sendCancelRequest(final long taskId, final String reason, final ActionListener<CancelTasksResponse> listener) {
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTargetTaskId(new TaskId(clusterService.localNode().getId(), taskId));
        request.setReason(reason);
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
     * {@link AllocatedPersistentTask#updatePersistentTaskState} instead
     */
    void sendUpdateStateRequest(
        final String taskId,
        final long taskAllocationID,
        final PersistentTaskState taskState,
        final ActionListener<PersistentTask<?>> listener
    ) {
        UpdatePersistentTaskStatusAction.Request request = new UpdatePersistentTaskStatusAction.Request(
            taskId,
            taskAllocationID,
            taskState
        );
        execute(request, UpdatePersistentTaskStatusAction.INSTANCE, listener);
    }

    /**
     * Notifies the master node to remove a persistent task from the cluster state
     */
    public void sendRemoveRequest(final String taskId, final ActionListener<PersistentTask<?>> listener) {
        RemovePersistentTaskAction.Request request = new RemovePersistentTaskAction.Request(taskId);
        execute(request, RemovePersistentTaskAction.INSTANCE, listener);
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
    public void waitForPersistentTaskCondition(
        final String taskId,
        final Predicate<PersistentTask<?>> predicate,
        final @Nullable TimeValue timeout,
        final WaitForPersistentTaskListener<?> listener
    ) {
        ClusterStateObserver.waitForState(clusterService, threadPool.getThreadContext(), new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                listener.onResponse(PersistentTasksCustomMetadata.getTaskWithId(state, taskId));
            }

            @Override
            public void onClusterServiceClose() {
                listener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                listener.onTimeout(timeout);
            }
        }, clusterState -> predicate.test(PersistentTasksCustomMetadata.getTaskWithId(clusterState, taskId)), timeout, logger);
    }

    /**
     * Waits for persistent tasks to comply with a given predicate, then call back the listener accordingly.
     *
     * @param predicate the predicate to evaluate
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTasksCondition(
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
        }, clusterState -> predicate.test(clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE)), timeout, logger);
    }

    public interface WaitForPersistentTaskListener<P extends PersistentTaskParams> extends ActionListener<PersistentTask<P>> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("Timed out when waiting for persistent task after " + timeout));
        }
    }
}
