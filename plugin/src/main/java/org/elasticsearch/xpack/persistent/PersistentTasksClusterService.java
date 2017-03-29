/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.Objects;

/**
 * Component that runs only on the master node and is responsible for assigning running tasks to nodes
 */
public class PersistentTasksClusterService extends AbstractComponent implements ClusterStateListener {

    private final ClusterService clusterService;
    private final PersistentTasksExecutorRegistry registry;

    public PersistentTasksClusterService(Settings settings, PersistentTasksExecutorRegistry registry, ClusterService clusterService) {
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
    public <Request extends PersistentTaskRequest> void createPersistentTask(String action, Request request,
                                                                             ActionListener<Long> listener) {
        clusterService.submitStateUpdateTask("create persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                validate(action, clusterService.state(), request);
                final Assignment assignment;
                assignment = getAssignement(action, currentState, request);
                return update(currentState, builder(currentState).addTask(action, request, assignment));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(
                        ((PersistentTasksCustomMetaData) newState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE)).getCurrentId());
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
    public void completePersistentTask(long id, Exception failure, ActionListener<Empty> listener) {
        final String source;
        if (failure != null) {
            logger.warn("persistent task " + id + " failed", failure);
            source = "finish persistent task (failed)";
        } else {
            source = "finish persistent task (success)";
        }
        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id)) {
                    tasksInProgress.finishTask(id);
                    return update(currentState, tasksInProgress);
                } else {
                    // we don't send the error message back to the caller becase that would cause an infinite loop of notifications
                    logger.warn("The task {} wasn't found, status is not updated", id);
                    return currentState;
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
     * Switches the persistent task from stopped to started mode
     *
     * @param id       the id of a persistent task
     * @param listener the listener that will be called when task is removed
     */
    public void startPersistentTask(long id, ActionListener<Empty> listener) {
        clusterService.submitStateUpdateTask("start persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id)) {
                    return update(currentState, tasksInProgress
                            .assignTask(id, (action, request) -> getAssignement(action, currentState, request)));
                } else {
                    throw new ResourceNotFoundException("the task with id {} doesn't exist", id);
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
     * Removes the persistent task
     *
     * @param id       the id of a persistent task
     * @param listener the listener that will be called when task is removed
     */
    public void removePersistentTask(long id, ActionListener<Empty> listener) {
        clusterService.submitStateUpdateTask("remove persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id)) {
                    return update(currentState, tasksInProgress.removeTask(id));
                } else {
                    throw new ResourceNotFoundException("the task with id {} doesn't exist", id);
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
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id)) {
                    return update(currentState, tasksInProgress.updateTaskStatus(id, status));
                } else {
                    throw new ResourceNotFoundException("the task with id {} doesn't exist", id);
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

    private <Request extends PersistentTaskRequest> Assignment getAssignement(String taskName, ClusterState currentState, Request request) {
        PersistentTasksExecutor<Request> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);
        return persistentTasksExecutor.getAssignment(request, currentState);
    }

    private <Request extends PersistentTaskRequest> void validate(String taskName, ClusterState currentState, Request request) {
        PersistentTasksExecutor<Request> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);
        persistentTasksExecutor.validate(request, currentState);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            logger.trace("checking task reassignment for cluster state {}", event.state().getVersion());
            if (reassignmentRequired(event, this::getAssignement)) {
                logger.trace("task reassignment is needed");
                reassignTasks();
            } else {
                logger.trace("task reassignment is not needed");
            }
        }
    }

    interface ExecutorNodeDecider {
        <Request extends PersistentTaskRequest> Assignment getAssignment(String action, ClusterState currentState, Request request);
    }

    static boolean reassignmentRequired(ClusterChangedEvent event, ExecutorNodeDecider decider) {
        PersistentTasksCustomMetaData tasks = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData prevTasks = event.previousState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (tasks != null && (Objects.equals(tasks, prevTasks) == false ||
                event.nodesChanged() ||
                event.routingTableChanged() ||
                event.previousState().nodes().isLocalNodeElectedMaster() == false)) {
            // We need to check if removed nodes were running any of the tasks and reassign them
            boolean reassignmentRequired = false;
            for (PersistentTask<?> taskInProgress : tasks.tasks()) {
                if (taskInProgress.needsReassignment(event.state().nodes())) {
                    // there is an unassigned task or task with a disappeared node - we need to try assigning it
                    if (Objects.equals(taskInProgress.getAssignment(),
                            decider.getAssignment(taskInProgress.getTaskName(), event.state(), taskInProgress.getRequest())) == false) {
                        // it looks like a assignment for at least one task is possible - let's trigger reassignment
                        reassignmentRequired = true;
                        break;
                    }

                }
            }
            return reassignmentRequired;
        }
        return false;
    }

    /**
     * Evaluates the cluster state and tries to assign tasks to nodes
     */
    public void reassignTasks() {
        clusterService.submitStateUpdateTask("reassign persistent tasks", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return reassignTasks(currentState, logger, PersistentTasksClusterService.this::getAssignement);
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

    static ClusterState reassignTasks(ClusterState currentState, Logger logger, ExecutorNodeDecider decider) {
        PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        ClusterState clusterState = currentState;
        DiscoveryNodes nodes = currentState.nodes();
        if (tasks != null) {
            logger.trace("reassigning {} persistent tasks", tasks.tasks().size());
            // We need to check if removed nodes were running any of the tasks and reassign them
            for (PersistentTask<?> task : tasks.tasks()) {
                if (task.needsReassignment(nodes)) {
                    // there is an unassigned task - we need to try assigning it
                    Assignment assignment = decider.getAssignment(task.getTaskName(), clusterState, task.getRequest());
                    if (Objects.equals(assignment, task.getAssignment()) == false) {
                        logger.trace("reassigning task {} from node {} to node {}", task.getId(),
                                task.getAssignment().getExecutorNode(), assignment.getExecutorNode());
                        clusterState = update(clusterState, builder(clusterState).reassignTask(task.getId(), assignment));
                    } else {
                        logger.trace("ignoring task {} because assignment is the same {}", task.getId(), assignment);
                    }
                } else {
                    logger.trace("ignoring task {} because it is still running", task.getId());
                }
            }
        }
        return clusterState;
    }

    private static PersistentTasksCustomMetaData.Builder builder(ClusterState currentState) {
        return PersistentTasksCustomMetaData.builder(currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
    }

    private static ClusterState update(ClusterState currentState, PersistentTasksCustomMetaData.Builder tasksInProgress) {
        if (tasksInProgress.isChanged()) {
            return ClusterState.builder(currentState).metaData(
                    MetaData.builder(currentState.metaData()).putCustom(PersistentTasksCustomMetaData.TYPE, tasksInProgress.build())
            ).build();
        } else {
            return currentState;
        }
    }
}