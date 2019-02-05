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

import org.apache.logging.log4j.LogManager;
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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.persistent.decider.AssignmentDecision;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;

/**
 * Component that runs only on the master node and is responsible for assigning running tasks to nodes
 */
public class PersistentTasksClusterService implements ClusterStateListener, Closeable {

    public static final Setting<TimeValue> CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING =
        Setting.timeSetting("cluster.persistent_tasks.allocation.recheck_interval", TimeValue.timeValueSeconds(30),
            TimeValue.timeValueSeconds(10), Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static final Logger logger = LogManager.getLogger(PersistentTasksClusterService.class);

    private final ClusterService clusterService;
    private final PersistentTasksExecutorRegistry registry;
    private final EnableAssignmentDecider decider;
    private final ThreadPool threadPool;
    private final PeriodicRechecker periodicRechecker;

    public PersistentTasksClusterService(Settings settings, PersistentTasksExecutorRegistry registry, ClusterService clusterService,
                                         ThreadPool threadPool) {
        this.clusterService = clusterService;
        this.registry = registry;
        this.decider = new EnableAssignmentDecider(settings, clusterService.getClusterSettings());
        this.threadPool = threadPool;
        this.periodicRechecker = new PeriodicRechecker(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING.get(settings));
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING,
            this::setRecheckInterval);
    }

    // visible for testing only
    public void setRecheckInterval(TimeValue recheckInterval) {
        periodicRechecker.setInterval(recheckInterval);
    }

    @Override
    public void close() {
        periodicRechecker.close();
    }

    /**
     * Creates a new persistent task on master node
     *
     * @param taskId     the task's id
     * @param taskName   the task's name
     * @param taskParams the task's parameters
     * @param listener   the listener that will be called when task is started
     */
    public <Params extends PersistentTaskParams> void createPersistentTask(String taskId, String taskName, Params taskParams,
                                                                           ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask("create persistent task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData.Builder builder = builder(currentState);
                if (builder.hasTask(taskId)) {
                    throw new ResourceAlreadyExistsException("task with id {" + taskId + "} already exist");
                }

                PersistentTasksExecutor<Params> taskExecutor = registry.getPersistentTaskExecutorSafe(taskName);
                taskExecutor.validate(taskParams, currentState);

                Assignment assignment = createAssignment(taskName, taskParams, currentState);
                return update(currentState, builder.addTask(taskId, taskName, taskParams, assignment));
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                PersistentTasksCustomMetaData tasks = newState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                if (tasks != null) {
                    PersistentTask<?> task = tasks.getTask(taskId);
                    listener.onResponse(task);
                    if (task != null && task.isAssigned() == false && periodicRechecker.isScheduled() == false) {
                        periodicRechecker.rescheduleIfNecessary();
                    }
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
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(id, allocationId)) {
                    tasksInProgress.removeTask(id);
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
            public ClusterState execute(ClusterState currentState) {
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
     * Update the state of a persistent task
     *
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param taskState        new state
     * @param listener         the listener that will be called when task is removed
     */
    public void updatePersistentTaskState(final String taskId,
                                          final long taskAllocationId,
                                          final PersistentTaskState taskState,
                                          final ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask("update task state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(taskId, taskAllocationId)) {
                    return update(currentState, tasksInProgress.updateTaskState(taskId, taskState));
                } else {
                    if (tasksInProgress.hasTask(taskId)) {
                        logger.warn("trying to update state on task {} with unexpected allocation id {}", taskId, taskAllocationId);
                    } else {
                        logger.warn("trying to update state on non-existing task {}", taskId);
                    }
                    throw new ResourceNotFoundException("the task with id {} and allocation id {} doesn't exist", taskId, taskAllocationId);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(newState, taskId));
            }
        });
    }

    /**
     * This unassigns a task from any node, i.e. it is assigned to a {@code null} node with the provided reason.
     *
     * Since the assignment executor node is null, the {@link PersistentTasksClusterService} will attempt to reassign it to a valid
     * node quickly.
     *
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param reason           the reason for unassigning the task from any node
     * @param listener         the listener that will be called when task is unassigned
     */
    public void unassignPersistentTask(final String taskId,
                                       final long taskAllocationId,
                                       final String reason,
                                       final ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask("unassign persistent task from any node", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                PersistentTasksCustomMetaData.Builder tasksInProgress = builder(currentState);
                if (tasksInProgress.hasTask(taskId, taskAllocationId)) {
                    logger.trace("Unassigning task {} with allocation id {}", taskId, taskAllocationId);
                    return update(currentState, tasksInProgress.reassignTask(taskId, unassignedAssignment(reason)));
                } else {
                    throw new ResourceNotFoundException("the task with id {} and allocation id {} doesn't exist", taskId, taskAllocationId);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                listener.onResponse(PersistentTasksCustomMetaData.getTaskWithId(newState, taskId));
            }
        });
    }

    /**
     * Creates a new {@link Assignment} for the given persistent task.
     *
     * @param taskName the task's name
     * @param taskParams the task's parameters
     * @param currentState the current {@link ClusterState}

     * @return a new {@link Assignment}
     */
    private <Params extends PersistentTaskParams> Assignment createAssignment(final String taskName,
                                                                              final Params taskParams,
                                                                              final ClusterState currentState) {
        PersistentTasksExecutor<Params> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);

        AssignmentDecision decision = decider.canAssign();
        if (decision.getType() == AssignmentDecision.Type.NO) {
            return unassignedAssignment("persistent task [" + taskName + "] cannot be assigned [" + decision.getReason() + "]");
        }

        return persistentTasksExecutor.getAssignment(taskParams, currentState);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            if (shouldReassignPersistentTasks(event)) {
                // We want to avoid a periodic check duplicating this work
                periodicRechecker.cancel();
                logger.trace("checking task reassignment for cluster state {}", event.state().getVersion());
                reassignPersistentTasks();
            }
        }
    }

    /**
     * Submit a cluster state update to reassign any persistent tasks that need reassigning
     */
    private void reassignPersistentTasks() {
        clusterService.submitStateUpdateTask("reassign persistent tasks", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return reassignTasks(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to reassign persistent tasks", e);
                // There must be a task that's worth rechecking because there was one
                // that caused this method to be called and the method failed to assign it
                periodicRechecker.rescheduleIfNecessary();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (isAnyTaskUnassigned(newState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE))) {
                    periodicRechecker.rescheduleIfNecessary();
                }
            }
        });
    }

    /**
     * Returns true if the cluster state change(s) require to reassign some persistent tasks. It can happen in the following
     * situations: a node left or is added, the routing table changed, the master node changed, the metadata changed or the
     * persistent tasks changed.
     */
    boolean shouldReassignPersistentTasks(final ClusterChangedEvent event) {
        final PersistentTasksCustomMetaData tasks = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (tasks == null) {
            return false;
        }

        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedMaster() == false;

        if (persistentTasksChanged(event)
            || event.nodesChanged()
            || event.routingTableChanged()
            || event.metaDataChanged()
            || masterChanged) {

            for (PersistentTask<?> task : tasks.tasks()) {
                if (needsReassignment(task.getAssignment(), event.state().nodes())) {
                    Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), event.state());
                    if (Objects.equals(assignment, task.getAssignment()) == false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if any persistent task is unassigned.
     */
    private boolean isAnyTaskUnassigned(final PersistentTasksCustomMetaData tasks) {
        return tasks != null && tasks.tasks().stream().anyMatch(task -> task.getAssignment().isAssigned() == false);
    }

    /**
     * Evaluates the cluster state and tries to assign tasks to nodes.
     *
     * @param currentState the cluster state to analyze
     * @return an updated version of the cluster state
     */
    ClusterState reassignTasks(final ClusterState currentState) {
        ClusterState clusterState = currentState;

        final PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (tasks != null) {
            logger.trace("reassigning {} persistent tasks", tasks.tasks().size());
            final DiscoveryNodes nodes = currentState.nodes();

            // We need to check if removed nodes were running any of the tasks and reassign them
            for (PersistentTask<?> task : tasks.tasks()) {
                if (needsReassignment(task.getAssignment(), nodes)) {
                    Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), clusterState);
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

    /** Returns true if the persistent tasks are not equal between the previous and the current cluster state **/
    static boolean persistentTasksChanged(final ClusterChangedEvent event) {
        String type = PersistentTasksCustomMetaData.TYPE;
        return Objects.equals(event.state().metaData().custom(type), event.previousState().metaData().custom(type)) == false;
    }

    /** Returns true if the task is not assigned or is assigned to a non-existing node */
    public static boolean needsReassignment(final Assignment assignment, final DiscoveryNodes nodes) {
        return (assignment.isAssigned() == false || nodes.nodeExists(assignment.getExecutorNode()) == false);
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

    private static Assignment unassignedAssignment(String reason) {
        return new Assignment(null, reason);
    }

    /**
     * Class to periodically try to reassign unassigned persistent tasks.
     */
    private class PeriodicRechecker extends AbstractAsyncTask {

        PeriodicRechecker(TimeValue recheckInterval) {
            super(logger, threadPool, recheckInterval, false);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            if (clusterService.localNode().isMasterNode()) {
                final ClusterState state = clusterService.state();
                logger.trace("periodic persistent task assignment check running for cluster state {}", state.getVersion());
                if (isAnyTaskUnassigned(state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE))) {
                    reassignPersistentTasks();
                }
            }
        }

        @Override
        public String toString() {
            return "persistent_task_recheck";
        }
    }
}
