/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cockroach;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cockroach.CockroachTasksInProgress.CockroachTaskInProgress;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Component that runs only on the master node and is responsible for moving running task from one state to another
 */
public class CockroachActionCoordinator extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private final CockroachActionRegistry cockroachActionRegistry;

    public CockroachActionCoordinator(Settings settings, ClusterService clusterService, CockroachActionRegistry cockroachActionRegistry) {
        super(settings);
        this.clusterService = clusterService;
        this.cockroachActionRegistry = cockroachActionRegistry;
    }

    /**
     * Creates a new cockroach task on master node
     *
     * @param callerTaskId task id of the task that started the task and waiting for its response
     * @param action       the action name
     * @param callerNodeId the node id of the caller node
     * @param request      request
     * @param listener     the listener that will be called when task is started
     */
    public <Request extends CockroachRequest<Request>, Response extends CockroachResponse> void createCockroachTask(
        final TaskId callerTaskId, String action, String callerNodeId, Request request, ActionListener<Empty> listener) {

        clusterService.submitStateUpdateTask("create cockroach task", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                TransportCockroachAction<Request, Response> cockroachAction = cockroachActionRegistry.getCockroachActionSafe(action);
                cockroachAction.validate(request, currentState);
                // Pick executor node to execute the request on
                DiscoveryNode executorNode = cockroachAction.executorNode(request, currentState);
                if (executorNode == null) {
                    // TODO: Implement retry machanism that would wait for an executor node to reappear
                    throw new IllegalStateException("No nodes available to execute the cockroach action [" + action + "], ignoring");
                }
                // If the caller node still alive - use it as a response node, otherwise pick another node
                DiscoveryNode responseNode = currentState.nodes().get(callerNodeId);
                if (responseNode == null) {
                    responseNode = cockroachAction.responseNode(request, currentState);
                    if (responseNode == null) {
                        throw new IllegalStateException("No nodes available to receive response for the cockroach action [" +
                            action + "], ignoring");
                    }
                }

                CockroachTasksInProgress cockroachTasksInProgress = currentState.custom(CockroachTasksInProgress.TYPE);
                final List<CockroachTaskInProgress> currentTasks = new ArrayList<>();
                if (cockroachTasksInProgress != null) {
                    currentTasks.addAll(cockroachTasksInProgress.entries());
                }
                CockroachTaskInProgress<Request, Response> task =
                    new CockroachTaskInProgress<>(UUIDs.randomBase64UUID(), action, request, executorNode.getId(), responseNode.getId(),
                        callerTaskId);
                currentTasks.add(task);
                ClusterState.Builder builder = ClusterState.builder(currentState);
                return builder.putCustom(CockroachTasksInProgress.TYPE, new CockroachTasksInProgress(currentTasks)).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(Empty.INSTANCE);
            }
        });
    }


    /**
     * Sets a response or a failure for a running cockroach task
     *
     * @param uuid     uuid of the task that should be updated
     * @param response response for the cockroach task if it finished successfully
     * @param failure  failure of the cockroach task if it failed
     * @param listener the listener that will be called when task is updated
     */
    @SuppressWarnings("unchecked")
    public <Response extends CockroachResponse> void finishCockroachTask(String uuid, @Nullable Response response,
                                                                         @Nullable Exception failure,
                                                                         ActionListener<Empty> listener) {
        performOperationOnCockroachTaskInProgress("finish cockroach task", uuid,
            taskInProgress -> new CockroachTaskInProgress(taskInProgress, response, failure), listener);
    }

    /**
     * Changes the caller node for a running cockroach task
     *
     * @param uuid       uuid of the task that should be updated
     * @param callerNode the new caller node id
     * @param listener   the listener that will be called when task is updated
     */
    public void updateCockroachTaskCaller(String uuid, String callerNode, ActionListener<Empty> listener) {
        performOperationOnCockroachTaskInProgress("update cockroach task caller", uuid,
            taskInProgress -> new CockroachTaskInProgress<>(taskInProgress, taskInProgress.getExecutorNode(), callerNode),
            listener);
    }

    /**
     * Changes the executor node for a running cockroach task
     *
     * @param uuid         uuid of the task that should be updated
     * @param executorNode the new executor node id
     * @param listener     the listener that will be called when task is updated
     */
    public void updateCockroachTaskExecutor(String uuid, String executorNode, ActionListener<Empty> listener) {
        performOperationOnCockroachTaskInProgress("update cockroach task executor", uuid,
            taskInProgress -> new CockroachTaskInProgress<>(taskInProgress, executorNode, taskInProgress.getCallerNode()),
            listener);
    }

    private <Request extends CockroachRequest<Request>, Response extends CockroachResponse> void performOperationOnCockroachTaskInProgress(
        String source, String uuid,
        Function<CockroachTaskInProgress<Request, Response>, CockroachTaskInProgress<Request, Response>> operation,
        ActionListener<Empty> listener) {

        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                CockroachTasksInProgress cockroachTasksInProgress = currentState.custom(CockroachTasksInProgress.TYPE);
                if (cockroachTasksInProgress == null) {
                    logger.warn("Trying to update cockroach task with id {} that no longer exists, new master?", uuid);
                    return currentState;
                }
                final List<CockroachTaskInProgress> currentTasks = new ArrayList<>();
                boolean found = false;
                for (CockroachTaskInProgress taskInProgress : cockroachTasksInProgress.entries()) {
                    if (found == false && uuid.equals(taskInProgress.getUuid())) {
                        found = true;
                        //noinspection unchecked
                        CockroachTaskInProgress newTask = operation.apply(taskInProgress);
                        if (newTask != null) {
                            currentTasks.add(newTask);
                        }
                    } else {
                        currentTasks.add(taskInProgress);
                    }
                }
                if (found) {
                    ClusterState.Builder builder = ClusterState.builder(currentState);
                    return builder.putCustom(CockroachTasksInProgress.TYPE, new CockroachTasksInProgress(currentTasks)).build();
                } else {
                    logger.warn("Trying to {} cockroach task with id {} that no longer exists", source, uuid);
                    return currentState;
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (listener != null) {
                    listener.onResponse(Empty.INSTANCE);
                }
            }

        });

    }

    /**
     * Removes a record about a running cockroach task from cluster state
     *
     * @param uuid     the uuid of a cockroach task
     * @param listener the listener that will be called when task is removed
     */
    public void removeCockroachTask(String uuid, ActionListener<Empty> listener) {
        performOperationOnCockroachTaskInProgress("remove cockroach task", uuid, taskInProgress -> null, listener);
    }


    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            CockroachTasksInProgress tasks = event.state().custom(CockroachTasksInProgress.TYPE);
            if (tasks != null && event.nodesRemoved()) {
                // We need to check if removed nodes were running any of the tasks and reassign them
                Set<String> removedNodes = event.nodesDelta().removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
                for (CockroachTaskInProgress taskInProgress : tasks.entries()) {
                    if (removedNodes.contains(taskInProgress.getCallerNode())) {
                        // The caller node disappeared, we need to assign a new caller node
                        String action = taskInProgress.getAction();
                        TransportCockroachAction cockroachAction = cockroachActionRegistry.getCockroachActionSafe(action);
                        DiscoveryNode node = cockroachAction.responseNode(taskInProgress.getRequest(), event.state());
                        if (node == null) {
                            // no nodes are available to receive the response - just remove the task
                            removeCockroachTask(taskInProgress.getUuid(), null);
                        } else {
                            updateCockroachTaskCaller(taskInProgress.getUuid(), node.getId(), null);
                        }
                    }
                    if (taskInProgress.isFinished() == false && removedNodes.contains(taskInProgress.getExecutorNode())) {
                        // The executor node disappeared before it had a chance to finish the task
                        // we need to assign it to a new node
                        String action = taskInProgress.getAction();
                        TransportCockroachAction cockroachAction = cockroachActionRegistry.getCockroachActionSafe(action);
                        DiscoveryNode node = cockroachAction.executorNode(taskInProgress.getRequest(), event.state());
                        if (node == null) {
                            // no nodes are available to perform the action - just fail it
                            // TODO: Implement retry mechanism that would wait for an executor node to reappear
                            finishCockroachTask(taskInProgress.getUuid(), null,
                                new IllegalStateException("No nodes available to execute the cockroach action [" + action + "]"),
                                null);
                        } else {
                            updateCockroachTaskExecutor(taskInProgress.getUuid(), node.getId(), null);
                        }
                    }
                }
            }
        }
    }
}
