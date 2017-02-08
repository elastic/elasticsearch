/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Component that runs only on the master node and is responsible for assigning running tasks to nodes
 */
public class PersistentTaskClusterService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private final PersistentActionRegistry registry;

    public PersistentTaskClusterService(Settings settings, PersistentActionRegistry registry, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        clusterService.addListener(this);
        this.registry = registry;

    }

    /**
     * Creates a new persistent task on master node
     *
     * @param action   the action name
     * @param request  request
     * @param listener the listener that will be called when task is started
     */
    public <Request extends PersistentActionRequest> void createPersistentTask(String action, Request request,
                                                                               ActionListener<Long> listener) {
        clusterService.submitStateUpdateTask("create persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                final String executorNodeId = executorNode(action, currentState, request);
                PersistentTasksInProgress tasksInProgress = currentState.custom(PersistentTasksInProgress.TYPE);
                long nextId;
                if (tasksInProgress != null) {
                    nextId = tasksInProgress.getCurrentId() + 1;
                } else {
                    nextId = 1;
                }
                return createPersistentTask(currentState, new PersistentTaskInProgress<>(nextId, action, request, executorNodeId));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(((PersistentTasksInProgress) newState.custom(PersistentTasksInProgress.TYPE)).getCurrentId());
            }
        });
    }


    /**
     * Restarts a record about a running persistent task from cluster state
     *
     * @param id       the id of a persistent task
     * @param failure  the reason for restarting the task or null if the task completed successfully
     * @param listener the listener that will be called when task is removed
     */
    public void completeOrRestartPersistentTask(long id, Exception failure, ActionListener<Empty> listener) {
        final String source;
        if (failure != null) {
            logger.warn("persistent task " + id + " failed, restarting", failure);
            source = "restart persistent task";
        } else {
            source = "finish persistent task";
        }
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksInProgress tasksInProgress = currentState.custom(PersistentTasksInProgress.TYPE);
                if (tasksInProgress == null) {
                    // Nothing to do, the task was already deleted
                    return currentState;
                }
                if (failure != null) {
                    // If the task failed - we need to restart it on another node, otherwise we just remove it
                    PersistentTaskInProgress<?> taskInProgress = tasksInProgress.getTask(id);
                    if (taskInProgress != null) {
                        String executorNode = executorNode(taskInProgress.getAction(), currentState, taskInProgress.getRequest());
                        return updatePersistentTask(currentState, new PersistentTaskInProgress<>(taskInProgress, executorNode));
                    }
                    return currentState;
                } else {
                    return removePersistentTask(currentState, id);
                }

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
     * Update task status
     *
     * @param id       the id of a persistent task
     * @param status   new status
     * @param listener the listener that will be called when task is removed
     */
    public void updatePersistentTaskStatus(long id, Task.Status status, ActionListener<Empty> listener) {
        clusterService.submitStateUpdateTask("update task status", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksInProgress tasksInProgress = currentState.custom(PersistentTasksInProgress.TYPE);
                if (tasksInProgress == null) {
                    // Nothing to do, the task no longer exists
                    return currentState;
                }
                PersistentTaskInProgress<?> task = tasksInProgress.getTask(id);
                if (task != null) {
                    return updatePersistentTask(currentState, new PersistentTaskInProgress<>(task, status));
                }
                return currentState;
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

    private ClusterState updatePersistentTask(ClusterState oldState, PersistentTaskInProgress<?> newTask) {
        PersistentTasksInProgress oldTasks = oldState.custom(PersistentTasksInProgress.TYPE);
        Map<Long, PersistentTaskInProgress<?>> taskMap = new HashMap<>();
        taskMap.putAll(oldTasks.taskMap());
        taskMap.put(newTask.getId(), newTask);
        ClusterState.Builder builder = ClusterState.builder(oldState);
        PersistentTasksInProgress newTasks = new PersistentTasksInProgress(oldTasks.getCurrentId(), Collections.unmodifiableMap(taskMap));
        return builder.putCustom(PersistentTasksInProgress.TYPE, newTasks).build();
    }

    private ClusterState createPersistentTask(ClusterState oldState, PersistentTaskInProgress<?> newTask) {
        PersistentTasksInProgress oldTasks = oldState.custom(PersistentTasksInProgress.TYPE);
        Map<Long, PersistentTaskInProgress<?>> taskMap = new HashMap<>();
        if (oldTasks != null) {
            taskMap.putAll(oldTasks.taskMap());
        }
        taskMap.put(newTask.getId(), newTask);
        ClusterState.Builder builder = ClusterState.builder(oldState);
        PersistentTasksInProgress newTasks = new PersistentTasksInProgress(newTask.getId(), Collections.unmodifiableMap(taskMap));
        return builder.putCustom(PersistentTasksInProgress.TYPE, newTasks).build();
    }

    private ClusterState removePersistentTask(ClusterState oldState, long taskId) {
        PersistentTasksInProgress oldTasks = oldState.custom(PersistentTasksInProgress.TYPE);
        if (oldTasks != null) {
            Map<Long, PersistentTaskInProgress<?>> taskMap = new HashMap<>();
            ClusterState.Builder builder = ClusterState.builder(oldState);
            taskMap.putAll(oldTasks.taskMap());
            taskMap.remove(taskId);
            PersistentTasksInProgress newTasks =
                    new PersistentTasksInProgress(oldTasks.getCurrentId(), Collections.unmodifiableMap(taskMap));
            return builder.putCustom(PersistentTasksInProgress.TYPE, newTasks).build();
        } else {
            // no tasks - nothing to do
            return oldState;
        }
    }

    private <Request extends PersistentActionRequest> String executorNode(String action, ClusterState currentState, Request request) {
        TransportPersistentAction<Request> persistentAction = registry.getPersistentActionSafe(action);
        persistentAction.validate(request, currentState);
        DiscoveryNode executorNode = persistentAction.executorNode(request, currentState);
        final String executorNodeId;
        if (executorNode == null) {
            // The executor node not available yet, we will create task with empty executor node and try
            // again later
            executorNodeId = null;
        } else {
            executorNodeId = executorNode.getId();
        }
        return executorNodeId;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            PersistentTasksInProgress tasks = event.state().custom(PersistentTasksInProgress.TYPE);
            if (tasks != null && (event.nodesChanged() || event.previousState().nodes().isLocalNodeElectedMaster() == false)) {
                // We need to check if removed nodes were running any of the tasks and reassign them
                boolean reassignmentRequired = false;
                Set<String> removedNodes = event.nodesDelta().removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
                for (PersistentTaskInProgress<?> taskInProgress : tasks.tasks()) {
                    if (taskInProgress.getExecutorNode() == null) {
                        // there is an unassigned task - we need to try assigning it
                        reassignmentRequired = true;
                        break;
                    }
                    if (removedNodes.contains(taskInProgress.getExecutorNode())) {
                        // The caller node disappeared, we need to assign a new caller node
                        reassignmentRequired = true;
                        break;
                    }
                }
                if (reassignmentRequired) {
                    reassignTasks();
                }
            }
        }
    }

    /**
     * Evaluates the cluster state and tries to assign tasks to nodes
     */
    public void reassignTasks() {
        clusterService.submitStateUpdateTask("reassign persistent tasks", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksInProgress tasks = currentState.custom(PersistentTasksInProgress.TYPE);
                ClusterState newClusterState = currentState;
                DiscoveryNodes nodes = currentState.nodes();
                if (tasks != null) {
                    // We need to check if removed nodes were running any of the tasks and reassign them
                    for (PersistentTaskInProgress<?> task : tasks.tasks()) {
                        if (task.getExecutorNode() == null || nodes.nodeExists(task.getExecutorNode()) == false) {
                            // there is an unassigned task - we need to try assigning it
                            String executorNode = executorNode(task.getAction(), currentState, task.getRequest());
                            if (Objects.equals(executorNode, task.getExecutorNode()) == false) {
                                newClusterState = updatePersistentTask(newClusterState, new PersistentTaskInProgress<>(task, executorNode));
                            }
                        }
                    }
                }
                return newClusterState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Unsuccessful persistent task reassignment", e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

            }
        });
    }
}