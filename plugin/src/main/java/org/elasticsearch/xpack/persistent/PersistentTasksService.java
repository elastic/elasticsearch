/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.function.Predicate;

import static org.elasticsearch.xpack.ClientHelper.PERSISTENT_TASK_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

/**
 * This service is used by persistent actions to propagate changes in the action state and notify about completion
 */
public class PersistentTasksService extends AbstractComponent {

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public PersistentTasksService(Settings settings, ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Creates the specified persistent task and attempts to assign it to a node.
     */
    @SuppressWarnings("unchecked")
    public <Params extends PersistentTaskParams> void startPersistentTask(String taskId, String taskName, @Nullable Params params,
                                                                          ActionListener<PersistentTask<Params>> listener) {
        StartPersistentTaskAction.Request createPersistentActionRequest =
                new StartPersistentTaskAction.Request(taskId, taskName, params);
        try {
            executeAsyncWithOrigin(client, PERSISTENT_TASK_ORIGIN, StartPersistentTaskAction.INSTANCE, createPersistentActionRequest,
                    ActionListener.wrap(o -> listener.onResponse((PersistentTask<Params>) o.getTask()), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Notifies the PersistentTasksClusterService about successful (failure == null) completion of a task or its failure
     */
    public void sendCompletionNotification(String taskId, long allocationId, Exception failure,
                                           ActionListener<PersistentTask<?>> listener) {
        CompletionPersistentTaskAction.Request restartRequest = new CompletionPersistentTaskAction.Request(taskId, allocationId, failure);
        try {
            executeAsyncWithOrigin(client, PERSISTENT_TASK_ORIGIN, CompletionPersistentTaskAction.INSTANCE, restartRequest,
                    ActionListener.wrap(o -> listener.onResponse(o.getTask()), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Cancels a locally running task using the task manager
     */
    void sendTaskManagerCancellation(long taskId, ActionListener<CancelTasksResponse> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(new TaskId(localNode.getId(), taskId));
        cancelTasksRequest.setReason("persistent action was removed");
        try {
            executeAsyncWithOrigin(client.threadPool().getThreadContext(), PERSISTENT_TASK_ORIGIN, cancelTasksRequest, listener,
                    client.admin().cluster()::cancelTasks);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Updates status of the persistent task.
     * <p>
     * Persistent task implementers shouldn't call this method directly and use
     * {@link AllocatedPersistentTask#updatePersistentStatus} instead
     */
    void updateStatus(String taskId, long allocationId, Task.Status status, ActionListener<PersistentTask<?>> listener) {
        UpdatePersistentTaskStatusAction.Request updateStatusRequest =
                new UpdatePersistentTaskStatusAction.Request(taskId, allocationId, status);
        try {
            executeAsyncWithOrigin(client, PERSISTENT_TASK_ORIGIN, UpdatePersistentTaskStatusAction.INSTANCE, updateStatusRequest,
                    ActionListener.wrap(o -> listener.onResponse(o.getTask()), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Cancels if needed and removes a persistent task
     */
    public void cancelPersistentTask(String taskId, ActionListener<PersistentTask<?>> listener) {
        RemovePersistentTaskAction.Request removeRequest = new RemovePersistentTaskAction.Request(taskId);
        try {
            executeAsyncWithOrigin(client, PERSISTENT_TASK_ORIGIN, RemovePersistentTaskAction.INSTANCE, removeRequest,
                ActionListener.wrap(o -> listener.onResponse(o.getTask()), listener::onFailure));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Checks if the persistent task with giving id (taskId) has the desired state and if it doesn't
     * waits of it.
     */
    public void waitForPersistentTaskStatus(String taskId, Predicate<PersistentTask<?>> predicate, @Nullable TimeValue timeout,
                                            WaitForPersistentTaskStatusListener<?> listener) {
        ClusterStateObserver stateObserver = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        if (predicate.test(PersistentTasksCustomMetaData.getTaskWithId(stateObserver.setAndGetObservedState(), taskId))) {
            listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(stateObserver.setAndGetObservedState(), taskId));
        } else {
            stateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(state, taskId));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, clusterState -> predicate.test(PersistentTasksCustomMetaData.getTaskWithId(clusterState, taskId)));
        }
    }

    public void waitForPersistentTasksStatus(Predicate<PersistentTasksCustomMetaData> predicate,
            @Nullable TimeValue timeout, ActionListener<Boolean> listener) {
        ClusterStateObserver stateObserver = new ClusterStateObserver(clusterService, timeout,
                logger, threadPool.getThreadContext());
        if (predicate.test(stateObserver.setAndGetObservedState().metaData().custom(PersistentTasksCustomMetaData.TYPE))) {
            listener.onResponse(true);
        } else {
            stateObserver.waitForNextChange(new ClusterStateObserver.Listener() {
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
                    listener.onFailure(new IllegalStateException("timed out after " + timeout));
                }
            }, clusterState -> predicate.test(clusterState.metaData().custom(PersistentTasksCustomMetaData.TYPE)), timeout);
        }
    }

    public interface WaitForPersistentTaskStatusListener<Params extends PersistentTaskParams>
            extends ActionListener<PersistentTask<Params>> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("timed out after " + timeout));
        }
    }
}