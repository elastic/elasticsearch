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

package org.elasticsearch.persistent;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;

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
     * @param params   params
     * @param listener the listener that will be called when task is started
     */
    public <Params extends PersistentTaskParams> void createPersistentTask(String taskId, String action, @Nullable Params params,
                                                                           ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask("create persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder builder = builder(currentState);
                if (builder.hasTask(taskId)) {
                    throw new ResourceAlreadyExistsException("task with id {" + taskId + "} already exist");
                }
                validate(action, currentState, params);
                final Assignment assignment;
                assignment = getAssignement(action, currentState, params);
                return update(currentState, builder.addTask(taskId, action, params, assignment));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                PersistentTasksCustomMetaData tasks = newState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                if (tasks != null) {
                    listener.onResponse(tasks.getTask(taskId));
                } else {
                    listener.onResponse(null);
                }
            }
        });
    }


    /**
     * Restarts a record about a running persistent task from cluster state
     *
     * @param id           the id of the persistent task
     * @param allocationId the allocation id of the persistent task
     * @param failure      the reason for restarting the task or null if the task completed successfully
     * @param listener     the listener that will be called when task is removed
     */
    public void completePersistentTask(String id, long allocationId, Exception failure, ActionListener<PersistentTask<?>> listener) {
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
                if (tasksInProgress.hasTask(id, allocationId)) {
                    tasksInProgress.finishTask(id);
                    return update(currentState, tasksInProgress);
                } else {
                    if (tasksInProgress.hasTask(id)) {
                        logger.warn("The task [{}] with id [{}] was found but it has a different allocation id [{}], status is not updated",
                                PersistentTasksCustomMetaData.getTaskWithId(currentState, id).getTaskName(), id, allocationId);
                    } else {
                        logger.warn("The task [{}] wasn't found, status is not updated", id);
                    }
                    throw new ResourceNotFoundException("the task with id [" + id + "] and allocation id [" + allocationId + "] not found");
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // Using old state since in the new state the task is already gone
                listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(oldState, id));
            }
        });
    }

    /**
     * Removes the persistent task
     *
     * @param id       the id of a persistent task
     * @param listener the listener that will be called when task is removed
     */
    public void removePersistentTask(String id, ActionListener<PersistentTask<?>> listener) {
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
                // Using old state since in the new state the task is already gone
                listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(oldState, id));
            }
        });
    }

    /**
     * Update task status
     *
     * @param id           the id of a persistent task
     * @param allocationId the expected allocation id of the persistent task
     * @param status       new status
     * @param listener     the listener that will be called when task is removed
     */
    public void updatePersistentTaskStatus(String id, long allocationId, Task.Status status, ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask("update task status", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id, allocationId)) {
                    return update(currentState, tasksInProgress.updateTaskStatus(id, status));
                } else {
                    if (tasksInProgress.hasTask(id)) {
                        logger.warn("trying to update status on task {} with unexpected allocation id {}", id, allocationId);
                    } else {
                        logger.warn("trying to update status on non-existing task {}", id);
                    }
                    throw new ResourceNotFoundException("the task with id {} and allocation id {} doesn't exist", id, allocationId);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(newState, id));
            }
        });
    }

    private <Params extends PersistentTaskParams> Assignment getAssignement(String taskName, ClusterState currentState,
                                                                            @Nullable Params params) {
        PersistentTasksExecutor<Params> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);
        return persistentTasksExecutor.getAssignment(params, currentState);
    }

    private <Params extends PersistentTaskParams> void validate(String taskName, ClusterState currentState, @Nullable Params params) {
        PersistentTasksExecutor<Params> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);
        persistentTasksExecutor.validate(params, currentState);
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
        <Params extends PersistentTaskParams> Assignment getAssignment(String action, ClusterState currentState, Params params);
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
                            decider.getAssignment(taskInProgress.getTaskName(), event.state(), taskInProgress.getParams())) == false) {
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
                    Assignment assignment = decider.getAssignment(task.getTaskName(), clusterState, task.getParams());
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
