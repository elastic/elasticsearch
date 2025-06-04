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
import org.elasticsearch.exception.ResourceAlreadyExistsException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.decider.AssignmentDecision;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.persistent.PersistentTasks.getAllTasks;
import static org.elasticsearch.persistent.PersistentTasks.taskTypeString;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.assertAllocationIdsConsistencyForOnePersistentTasks;
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.getNonZeroAllocationIds;

/**
 * Component that runs only on the master node and is responsible for assigning running tasks to nodes
 */
public final class PersistentTasksClusterService implements ClusterStateListener, Closeable {

    public static final Setting<TimeValue> CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.persistent_tasks.allocation.recheck_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(PersistentTasksClusterService.class);

    private final ClusterService clusterService;
    private final PersistentTasksExecutorRegistry registry;
    private final EnableAssignmentDecider enableDecider;
    private final ThreadPool threadPool;
    private final PeriodicRechecker periodicRechecker;
    private final AtomicBoolean reassigningTasks = new AtomicBoolean(false);

    public PersistentTasksClusterService(
        Settings settings,
        PersistentTasksExecutorRegistry registry,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.registry = registry;
        this.enableDecider = new EnableAssignmentDecider(settings, clusterService.getClusterSettings());
        this.threadPool = threadPool;
        this.periodicRechecker = new PeriodicRechecker(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING.get(settings));
        if (DiscoveryNode.isMasterNode(settings)) {
            clusterService.addListener(this);
        }
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING, this::setRecheckInterval);
    }

    // visible for testing only
    public void setRecheckInterval(TimeValue recheckInterval) {
        periodicRechecker.setInterval(recheckInterval);
    }

    // visible for testing only
    PeriodicRechecker getPeriodicRechecker() {
        return periodicRechecker;
    }

    @Override
    public void close() {
        periodicRechecker.close();
    }

    /**
     * Creates a new project-scoped persistent task on master node
     *
     * @param projectId  the project for which the task should be created
     * @param taskId     the task's id
     * @param taskName   the task's name
     * @param taskParams the task's parameters
     * @param listener   the listener that will be called when task is started
     */
    public <Params extends PersistentTaskParams> void createProjectPersistentTask(
        ProjectId projectId,
        String taskId,
        String taskName,
        Params taskParams,
        ActionListener<PersistentTask<?>> listener
    ) {
        createPersistentTask(Objects.requireNonNull(projectId), taskId, taskName, taskParams, listener);
    }

    /**
     * Creates a new cluster-scoped persistent task on master node
     *
     * @param taskId     the task's id
     * @param taskName   the task's name
     * @param taskParams the task's parameters
     * @param listener   the listener that will be called when task is started
     */
    public <Params extends PersistentTaskParams> void createClusterPersistentTask(
        String taskId,
        String taskName,
        Params taskParams,
        ActionListener<PersistentTask<?>> listener
    ) {
        createPersistentTask(null, taskId, taskName, taskParams, listener);
    }

    private <Params extends PersistentTaskParams> void createPersistentTask(
        @Nullable ProjectId projectId,
        String taskId,
        String taskName,
        Params taskParams,
        ActionListener<PersistentTask<?>> listener
    ) {
        submitUnbatchedTask("create persistent task " + taskName + " [" + taskId + "]", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // The allocationId is shared between cluster-scoped and project-scoped tasks from all projects.
                // Therefore new or reassigned task must begin with the current max.
                final var builder = builder(currentState, projectId).setLastAllocationId(currentState);
                if (builder.hasTask(taskId)) {
                    throw new ResourceAlreadyExistsException("task with id {" + taskId + "} already exist");
                }

                PersistentTasksExecutor<Params> taskExecutor = registry.getPersistentTaskExecutorSafe(taskName);
                assert (projectId == null && taskExecutor.scope() == PersistentTasksExecutor.Scope.CLUSTER)
                    || (projectId != null && taskExecutor.scope() == PersistentTasksExecutor.Scope.PROJECT)
                    : "inconsistent project-id [" + projectId + "] and task scope [" + taskExecutor.scope() + "]";
                taskExecutor.validate(taskParams, currentState);

                Assignment assignment = createAssignment(taskName, taskParams, currentState);
                logger.debug("creating {} persistent task [{}] with assignment [{}]", taskTypeString(projectId), taskName, assignment);
                return builder.addTask(taskId, taskName, taskParams, assignment).buildAndUpdate(currentState, projectId);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                final var tasks = PersistentTasks.getTasks(newState, projectId);
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

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    /**
     * Completes a record about a running persistent task from cluster state
     * The task may be either cluster-scope or project-scope. This is determined by the projectIdHint and the taskId
     * against the cluster state.
     *
     * @param projectIdHint hint of the project for which the task should be completed, {code null} for cluster-scope tasks.
     *                      See also {@link #maybeNullProjectIdForClusterTask}
     * @param id            the id of the persistent task
     * @param allocationId  the allocation id of the persistent task
     * @param failure       the reason for restarting the task or null if the task completed successfully
     * @param listener      the listener that will be called when task is removed
     */
    void completePersistentTask(
        @Nullable ProjectId projectIdHint,
        String id,
        long allocationId,
        Exception failure,
        ActionListener<PersistentTask<?>> listener
    ) {
        final String source;
        if (failure != null) {
            logger.warn(taskTypeString(projectIdHint) + " persistent task " + id + " failed", failure);
            source = "finish " + taskTypeString(projectIdHint) + " persistent task [" + id + "] (failed)";
        } else {
            source = "finish " + taskTypeString(projectIdHint) + " persistent task [" + id + "] (success)";
        }
        submitUnbatchedTask(source, new ClusterStateUpdateTask() {
            private volatile ProjectId projectId;

            @Override
            public ClusterState execute(ClusterState currentState) {
                projectId = maybeNullProjectIdForClusterTask(currentState, projectIdHint, id);
                final var tasksInProgress = builder(currentState, projectId);
                if (tasksInProgress.hasTask(id, allocationId)) {
                    tasksInProgress.removeTask(id);
                    return tasksInProgress.buildAndUpdate(currentState, projectId);
                } else {
                    if (tasksInProgress.hasTask(id)) {
                        logger.warn(
                            "The {} task [{}] with id [{}] was found but it has a different allocation id [{}], status is not updated",
                            taskTypeString(projectId),
                            tasksInProgress.getCurrentTasks().get(id).getTaskName(),
                            id,
                            allocationId
                        );
                    } else {
                        logger.warn("The {} task [{}] wasn't found, status is not updated", taskTypeString(projectId), id);
                    }
                    throw new ResourceNotFoundException(
                        "the " + taskTypeString(projectId) + " task with id [" + id + "] and allocation id [" + allocationId + "] not found"
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                // Using old state since in the new state the task is already gone
                final var tasks = PersistentTasks.getTasks(oldState, projectId);
                listener.onResponse(tasks == null ? null : tasks.getTask(id));
            }
        });
    }

    /**
     * Removes the persistent task
     * The task may be either cluster-scope or project-scope. This is determined by the projectIdHint and the taskId
     * against the cluster state.
     *
     * @param projectIdHint hint of the project for which the task should be removed, {@code null} for cluster-scope tasks
     *                      See also {@link #maybeNullProjectIdForClusterTask}
     * @param id       the id of a persistent task
     * @param listener the listener that will be called when task is removed
     */
    void removePersistentTask(@Nullable ProjectId projectIdHint, String id, ActionListener<PersistentTask<?>> listener) {
        submitUnbatchedTask("remove " + taskTypeString(projectIdHint) + " persistent task [" + id + "]", new ClusterStateUpdateTask() {
            private volatile ProjectId projectId;

            @Override
            public ClusterState execute(ClusterState currentState) {
                projectId = maybeNullProjectIdForClusterTask(currentState, projectIdHint, id);
                final var tasksInProgress = builder(currentState, projectId);
                if (tasksInProgress.hasTask(id)) {
                    return tasksInProgress.removeTask(id).buildAndUpdate(currentState, projectId);
                } else {
                    throw new ResourceNotFoundException("the " + taskTypeString(projectId) + " task with id {} doesn't exist", id);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                // Using old state since in the new state the task is already gone
                listener.onResponse(
                    Optional.ofNullable(PersistentTasks.getTasks(oldState, projectId)).map(tasks -> tasks.getTask(id)).orElse(null)
                );
            }
        });
    }

    /**
     * Update the state of a persistent task
     * The task may be either cluster-scope or project-scope. This is determined by the projectIdHint and the taskId
     * against the cluster state.
     *
     * @param projectIdHint    hint of the project for which the task should be updated, {@code null} for cluster-scope tasks
     *                         See also {@link #maybeNullProjectIdForClusterTask}
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param taskState        new state
     * @param listener         the listener that will be called when task is removed
     */
    void updatePersistentTaskState(
        @Nullable final ProjectId projectIdHint,
        final String taskId,
        final long taskAllocationId,
        final PersistentTaskState taskState,
        final ActionListener<PersistentTask<?>> listener
    ) {
        submitUnbatchedTask("update " + taskTypeString(projectIdHint) + " task state [" + taskId + "]", new ClusterStateUpdateTask() {
            private volatile ProjectId projectId;

            @Override
            public ClusterState execute(ClusterState currentState) {
                projectId = maybeNullProjectIdForClusterTask(currentState, projectIdHint, taskId);
                final var tasksInProgress = builder(currentState, projectId);
                if (tasksInProgress.hasTask(taskId, taskAllocationId)) {
                    return tasksInProgress.updateTaskState(taskId, taskState).buildAndUpdate(currentState, projectId);
                } else {
                    if (tasksInProgress.hasTask(taskId)) {
                        logger.warn(
                            "trying to update state on {} task {} with unexpected allocation id {}",
                            taskTypeString(projectId),
                            taskId,
                            taskAllocationId
                        );
                    } else {
                        logger.warn("trying to update state on non-existing {} task {}", taskTypeString(projectId), taskId);
                    }
                    throw new ResourceNotFoundException(
                        "the {} task with id {} and allocation id {} doesn't exist",
                        taskTypeString(projectId),
                        taskId,
                        taskAllocationId
                    );
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                listener.onResponse(
                    Optional.ofNullable(PersistentTasks.getTasks(newState, projectId)).map(tasks -> tasks.getTask(taskId)).orElse(null)
                );
            }
        });
    }

    /**
     * This unassigns task from any node, i.e. it is assigned to a {@code null} node with the provided reason.
     * The task may be either cluster-scope or project-scope. This is determined by the projectIdHint and the taskId
     * against the cluster state.
     *
     * Since the assignment executor node is null, the {@link PersistentTasksClusterService} will attempt to reassign it to a valid
     * node quickly.
     *
     * @param projectIdHint    hint of the project for which the task should be unassigned, {@code null} for cluster-scope tasks
     *                         See also {@link #maybeNullProjectIdForClusterTask}
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param reason           the reason for unassigning the task from any node
     * @param listener         the listener that will be called when task is unassigned
     */
    public void unassignPersistentTask(
        @Nullable final ProjectId projectIdHint,
        final String taskId,
        final long taskAllocationId,
        final String reason,
        final ActionListener<PersistentTask<?>> listener
    ) {
        submitUnbatchedTask(
            "unassign " + taskTypeString(projectIdHint) + " persistent task [" + taskId + "] from any node",
            new ClusterStateUpdateTask() {
                private volatile ProjectId projectId;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    projectId = maybeNullProjectIdForClusterTask(currentState, projectIdHint, taskId);
                    final var tasksInProgress = builder(currentState, projectId);
                    if (tasksInProgress.hasTask(taskId, taskAllocationId)) {
                        logger.trace("Unassigning {} task {} with allocation id {}", taskTypeString(projectId), taskId, taskAllocationId);
                        // The allocationId is shared between cluster-scoped and project-scoped tasks from all projects.
                        // Therefore new or reassigned task must begin with the current max.
                        return tasksInProgress.setLastAllocationId(currentState)
                            .reassignTask(taskId, unassignedAssignment(reason))
                            .buildAndUpdate(currentState, projectId);
                    } else {
                        throw new ResourceNotFoundException(
                            "the {} task with id {} and allocation id {} doesn't exist",
                            taskTypeString(projectId),
                            taskId,
                            taskAllocationId
                        );
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    listener.onResponse(
                        Optional.ofNullable(PersistentTasks.getTasks(newState, projectId)).map(tasks -> tasks.getTask(taskId)).orElse(null)
                    );
                }
            }
        );
    }

    /**
     * Creates a new {@link Assignment} for the given persistent task.
     *
     * @param taskName the task's name
     * @param taskParams the task's parameters
     * @param currentState the current {@link ClusterState}

     * @return a new {@link Assignment}
     */
    private <Params extends PersistentTaskParams> Assignment createAssignment(
        final String taskName,
        final Params taskParams,
        final ClusterState currentState
    ) {
        PersistentTasksExecutor<Params> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);

        AssignmentDecision decision = enableDecider.canAssign();
        if (decision.getType() == AssignmentDecision.Type.NO) {
            return unassignedAssignment("persistent task [" + taskName + "] cannot be assigned [" + decision.getReason() + "]");
        }

        // Filter all nodes that are marked as shutting down, because we do not
        // want to assign a persistent task to a node that will shortly be
        // leaving the cluster
        final List<DiscoveryNode> candidateNodes = currentState.nodes()
            .stream()
            .filter(dn -> currentState.metadata().nodeShutdowns().contains(dn.getId()) == false)
            .collect(Collectors.toCollection(ArrayList::new));
        // Task assignment should not rely on node order
        Randomness.shuffle(candidateNodes);

        final Assignment assignment = persistentTasksExecutor.getAssignment(taskParams, candidateNodes, currentState);
        assert assignment != null : "getAssignment() should always return an Assignment object, containing a node or a reason why not";
        assert (assignment.getExecutorNode() == null
            || currentState.metadata().nodeShutdowns().contains(assignment.getExecutorNode()) == false)
            : "expected task ["
                + taskName
                + "] to be assigned to a node that is not marked as shutting down, but "
                + assignment.getExecutorNode()
                + " is currently marked as shutting down";
        return assignment;
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
        } else {
            periodicRechecker.cancel();
        }
    }

    /**
     * Submit a cluster state update to reassign any persistent tasks that need reassigning
     */
    void reassignPersistentTasks() {
        if (this.reassigningTasks.compareAndSet(false, true) == false) {
            return;
        }
        submitUnbatchedTask("reassign persistent tasks", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return reassignTasks(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                reassigningTasks.set(false);
                logger.warn("failed to reassign persistent tasks", e);
                if (e instanceof NotMasterException == false) {
                    // There must be a task that's worth rechecking because there was one
                    // that caused this method to be called and the method failed to assign it,
                    // but only do this if the node is still the master
                    try {
                        periodicRechecker.rescheduleIfNecessary();
                    } catch (Exception e2) {
                        assert e2 instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e2;
                        logger.warn("failed to reschedule persistent tasks rechecker", e2);
                    }
                }
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                reassigningTasks.set(false);
                if (isAnyTaskUnassigned(getAllTasks(newState))) {
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
        final List<PersistentTasks> allTasks = PersistentTasks.getAllTasks(event.state()).map(Tuple::v2).toList();
        if (allTasks.isEmpty()) {
            return false;
        }

        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedMaster() == false;

        if (persistentTasksChanged(event)
            || event.nodesChanged()
            || event.routingTableChanged()
            || event.metadataChanged()
            || masterChanged) {

            for (PersistentTasks tasks : allTasks) {
                for (PersistentTask<?> task : tasks.tasks()) {
                    if (needsReassignment(task.getAssignment(), event.state().nodes())) {
                        Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), event.state());
                        if (Objects.equals(assignment, task.getAssignment()) == false) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if any persistent task is unassigned.
     */
    private static boolean isAnyTaskUnassigned(final Stream<Tuple<ProjectId, PersistentTasks>> projectIdTasksTuples) {
        return projectIdTasksTuples.flatMap(tasks -> tasks.v2().tasks().stream())
            .anyMatch(task -> task.getAssignment().isAssigned() == false);
    }

    /**
     * Evaluates the cluster state and tries to assign tasks to nodes.
     *
     * @param currentState the cluster state to analyze
     * @return an updated version of the cluster state
     */
    ClusterState reassignTasks(final ClusterState currentState) {
        ClusterState clusterState = currentState;

        clusterState = reassignClusterOrSingleProjectTasks(null, clusterState);
        for (var projectId : currentState.metadata().projects().keySet()) {
            clusterState = reassignClusterOrSingleProjectTasks(projectId, clusterState);
        }
        return clusterState;
    }

    private ClusterState reassignClusterOrSingleProjectTasks(@Nullable final ProjectId projectId, final ClusterState currentState) {
        ClusterState clusterState = currentState;

        final var tasks = PersistentTasks.getTasks(currentState, projectId);
        if (tasks != null) {
            logger.trace("reassigning {} {} persistent tasks", tasks.tasks().size(), taskTypeString(projectId));
            final DiscoveryNodes nodes = currentState.nodes();

            // We need to check if removed nodes were running any of the tasks and reassign them
            for (PersistentTask<?> task : tasks.tasks()) {
                if (needsReassignment(task.getAssignment(), nodes)) {
                    Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), clusterState);
                    if (Objects.equals(assignment, task.getAssignment()) == false) {
                        logger.trace(
                            "reassigning {} task {} from node {} to node {}",
                            taskTypeString(projectId),
                            task.getId(),
                            task.getAssignment().getExecutorNode(),
                            assignment.getExecutorNode()
                        );
                        clusterState = builder(clusterState, projectId).setLastAllocationId(clusterState)
                            .reassignTask(task.getId(), assignment)
                            .buildAndUpdate(clusterState, projectId);
                    } else {
                        logger.trace(
                            "ignoring {} task {} because assignment is the same {}",
                            taskTypeString(projectId),
                            task.getId(),
                            assignment
                        );
                    }
                } else {
                    logger.trace("ignoring {} task {} because it is still running", taskTypeString(projectId), task.getId());
                }
            }
        }
        return clusterState;
    }

    /** Returns true if the persistent tasks are not equal between the previous and the current cluster state **/
    static boolean persistentTasksChanged(final ClusterChangedEvent event) {
        if (Objects.equals(
            ClusterPersistentTasksCustomMetadata.get(event.state().metadata()),
            ClusterPersistentTasksCustomMetadata.get(event.previousState().metadata())
        ) == false) {
            return true;
        }

        final Set<ProjectId> previousProjectIds = event.previousState().metadata().projects().keySet();
        final Set<ProjectId> projectIds = event.state().metadata().projects().keySet();

        for (ProjectId projectId : projectIds) {
            if (previousProjectIds.contains(projectId)) {
                if (Objects.equals(
                    PersistentTasksCustomMetadata.get(event.state().metadata().getProject(projectId)),
                    PersistentTasksCustomMetadata.get(event.previousState().metadata().getProject(projectId))
                ) == false) {
                    return true;
                }
            } else {
                if (PersistentTasksCustomMetadata.get(event.state().metadata().getProject(projectId)) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Returns true if the task is not assigned or is assigned to a non-existing node */
    public static boolean needsReassignment(final Assignment assignment, final DiscoveryNodes nodes) {
        return (assignment.isAssigned() == false || nodes.nodeExists(assignment.getExecutorNode()) == false);
    }

    private static PersistentTasks.Builder<?> builder(ClusterState currentState, @Nullable ProjectId projectId) {
        if (projectId == null) {
            return ClusterPersistentTasksCustomMetadata.builder(ClusterPersistentTasksCustomMetadata.get(currentState.metadata()));
        } else {
            return PersistentTasksCustomMetadata.builder(
                PersistentTasksCustomMetadata.get(currentState.getMetadata().getProject(projectId))
            );
        }
    }

    @FixForMultiProject(description = "Consider formalize this into ProjectResolver")
    @Nullable
    static ProjectId resolveProjectIdHint(ProjectResolver projectResolver) {
        try {
            return projectResolver.getProjectId();
        } catch (Exception e) {
            // The project resolver can throw if (1) the request is received in a cluster context and
            // (2) the project resolver does not allow fallback project-id. We intentionally skip
            // the error and return a null project-id which will be handled accordingly by the callers.
            logger.debug("skipping error on resolving project-id", e);
            return null;
        }
    }

    /**
     * Determine whether the project-id should be null by cross-checking the task-id and its existence in the cluster state.
     * This is necessary because a cluster-scope task may come in with a fallback project-id which should not be used.
     * For project-scope tasks (or tasks that do not exist), the project-id will be returned as is (and does not guarantee
     * that the task exists in the specified project) i.e. project-scope tasks must have the project-id set correctly beforehand
     * and this method is a noop for project-scope tasks.
     */
    @Nullable
    private static ProjectId maybeNullProjectIdForClusterTask(ClusterState clusterState, @Nullable ProjectId projectIdHint, String taskId) {
        if (projectIdHint == null) {
            return null;
        }

        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(clusterState.metadata());
        if (clusterTasks != null) {
            if (clusterTasks.getTask(taskId) != null) {
                assert assertProjectIdHintIsTheDefaultProjectId(projectIdHint);
                logger.debug("task [{}] is cluster-scoped but project ID [{}] is non-null, ignoring the project ID", taskId, projectIdHint);
                return null;
            }
        }
        return projectIdHint;
    }

    @FixForMultiProject(description = "consider removing this assertion if we want to allow cluster API to be called via project URLs")
    private static boolean assertProjectIdHintIsTheDefaultProjectId(ProjectId projectIdHint) {
        assert Metadata.DEFAULT_PROJECT_ID.equals(projectIdHint)
            : "expected default project ID when ignoring persistent task project ID hint but got " + projectIdHint;
        return true;
    }

    @FixForMultiProject(description = "Revisit this method to either reduce the complexity or remove if it turns out to be unnecessary")
    static boolean assertAllocationIdsConsistency(ClusterState clusterState) {
        // Cluster scoped persistent tasks and each project's project scoped persistent tasks should have no duplicated allocationId
        // The lastAllocationId must be larger or equal to allocationId of all individual tasks
        final var clusterPersistentTasksCustomMetadata = ClusterPersistentTasksCustomMetadata.get(clusterState.metadata());
        assert assertAllocationIdsConsistencyForOnePersistentTasks(clusterPersistentTasksCustomMetadata);

        final List<PersistentTasks> allPersistentTasks = new ArrayList<>();
        allPersistentTasks.add(clusterPersistentTasksCustomMetadata);

        final List<ProjectMetadata> projects = List.copyOf(clusterState.metadata().projects().values());
        for (ProjectMetadata projectMetadata : projects) {
            final var projectPersistentTasksCustomMetadata = PersistentTasksCustomMetadata.get(projectMetadata);
            assert assertAllocationIdsConsistencyForOnePersistentTasks(projectPersistentTasksCustomMetadata);
            allPersistentTasks.add(projectPersistentTasksCustomMetadata);
        }

        // No duplicated allocationId between cluster scoped and project scoped tasks across all projects
        for (int i = 0; i < allPersistentTasks.size() - 1; i++) {
            final PersistentTasks persistentTasks1 = allPersistentTasks.get(i);
            if (persistentTasks1 == null) {
                continue;
            }
            for (int j = i + 1; j < allPersistentTasks.size(); j++) {
                final PersistentTasks persistentTasks2 = allPersistentTasks.get(j);
                if (persistentTasks2 == null) {
                    continue;
                }
                assert Sets.intersection(
                    Set.copyOf(getNonZeroAllocationIds(persistentTasks1)),
                    Set.copyOf(getNonZeroAllocationIds(persistentTasks2))
                ).isEmpty()
                    : "duplicated allocationId found between "
                        + (i == 0 ? "cluster" : "project [" + projects.get(i - 1).id() + "]")
                        + " scoped persistent tasks ["
                        + persistentTasks1
                        + "] and project ["
                        + projects.get(j - 1).id()
                        + "] scoped persistent tasks ["
                        + persistentTasks2
                        + "]";
            }
        }
        return true;
    }

    static boolean assertUniqueTaskIdForClusterScopeTasks(ClusterState clusterState) {
        final var clusterTasks = ClusterPersistentTasksCustomMetadata.get(clusterState.metadata());
        if (clusterTasks == null) {
            return true;
        }
        final Set<String> clusterTaskIds = clusterTasks.taskMap().keySet();
        for (var projectId : clusterState.metadata().projects().keySet()) {
            final var projectTasks = PersistentTasks.getTasks(clusterState, projectId);
            if (projectTasks == null) {
                continue;
            }
            final var projectTaskIds = projectTasks.taskMap().keySet();
            final Set<String> intersection = Sets.intersection(clusterTaskIds, projectTaskIds);
            assert intersection.isEmpty() : "duplicated task ID found between cluster and project persistent tasks " + intersection;
        }
        return true;
    }

    private static Assignment unassignedAssignment(String reason) {
        return new Assignment(null, reason);
    }

    /**
     * Class to periodically try to reassign unassigned persistent tasks.
     */
    class PeriodicRechecker extends AbstractAsyncTask {

        PeriodicRechecker(TimeValue recheckInterval) {
            super(logger, threadPool, EsExecutors.DIRECT_EXECUTOR_SERVICE, recheckInterval, false);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            if (clusterService.localNode().isMasterNode()) {
                // TODO just run on the elected master?
                final ClusterState state = clusterService.state();
                logger.trace("periodic persistent task assignment check running for cluster state {}", state.getVersion());
                if (isAnyTaskUnassigned(PersistentTasks.getAllTasks(state))) {
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
