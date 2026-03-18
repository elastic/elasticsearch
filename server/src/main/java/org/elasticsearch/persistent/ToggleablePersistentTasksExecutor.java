/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/// A [PersistentTasksExecutor] whose lifecycle is gated by a boolean "enabled" flag.
///
/// On every cluster state update, the master node reconciles the desired state (enabled / disabled) with the
/// actual state (task exists / does not exist) and sends the appropriate start or remove request through
/// the [PersistentTasksService].
///
/// Subclasses control enablement by calling [#setEnabled(boolean)] or [#setEnabled(ProjectId, boolean)],
/// typically from a dynamic setting consumer.
///
/// Both [Scope#CLUSTER] and [Scope#PROJECT] scoped tasks are supported. For project-scoped
/// tasks, reconciliation runs independently for every project in the cluster state;
/// subclasses may override [#getProjectTaskId] to customize the per-project task identifier.
///
/// The class remains `abstract` only because [PersistentTasksExecutor#nodeOperation] is
/// inherited-abstract; no new abstract methods are introduced.
///
public abstract class ToggleablePersistentTasksExecutor<Params extends PersistentTaskParams> extends PersistentTasksExecutor<Params>
    implements
        ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ToggleablePersistentTasksExecutor.class);

    private final PersistentTasksService persistentTasksService;
    private final Supplier<Params> paramsFactory;

    private final ConcurrentHashMap<ProjectId, Boolean> perProjectEnabled = new ConcurrentHashMap<>();
    private volatile boolean clusterEnabled;

    @SuppressWarnings("this-escape")
    protected ToggleablePersistentTasksExecutor(
        String taskName,
        Executor executor,
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        boolean initiallyEnabled,
        Supplier<Params> paramsFactory
    ) {
        super(taskName, executor);
        this.persistentTasksService = persistentTasksService;
        this.paramsFactory = paramsFactory;
        this.clusterEnabled = initiallyEnabled;
        clusterService.addListener(this);
    }

    /// Returns the task identifier used for project-scoped tasks. Defaults to projectId/[#getTaskName()].
    /// Subclasses that support multi-project deployments should override this accordingly.
    protected String getProjectTaskId(ProjectId projectId) {
        return projectId + "/" + getTaskName();
    }

    /// Returns whether the task is globally enabled.
    protected boolean isEnabled() {
        return clusterEnabled;
    }

    /// Returns whether the task is enabled for the specified project. If [#setEnabled(ProjectId, boolean)] was
    /// never called for this project, defaults to the cluster-wide enabled flag.
    private boolean isEnabled(ProjectId projectId) {
        return perProjectEnabled.getOrDefault(projectId, clusterEnabled);
    }

    /// Sets the cluster enabled flag.
    protected void setEnabled(boolean enabled) {
        this.clusterEnabled = enabled;
    }

    /// Sets the enabled flag for the relevant project id.
    protected void setEnabled(ProjectId projectId, boolean enabled) {
        assert scope() == Scope.PROJECT : "Use setEnabled(boolean) when the task is cluster scoped";
        perProjectEnabled.put(projectId, enabled);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().getMasterNode() == null || event.state().clusterRecovered() == false) {
            return;
        }
        if (event.localNodeMaster()) {
            reconcile(event.state());
        }
    }

    /// Reconciles the desired task lifecycle state with the cluster state.
    /// Dispatches to cluster-level or per-project reconciliation based on [#scope()].
    private void reconcile(ClusterState state) {
        switch (scope()) {
            case Scope.CLUSTER -> reconcileClusterTask(state);
            case Scope.PROJECT -> {
                for (var projectMetadata : state.metadata().projects().values()) {
                    reconcileProjectTask(projectMetadata);
                }
                cleanupObsoleteProjectTasks(state);
            }
            default -> throw new IllegalArgumentException("Unsupported cluster scope");
        }
    }

    private void reconcileClusterTask(ClusterState state) {
        boolean taskExists = ClusterPersistentTasksCustomMetadata.getTaskWithId(state, getTaskName()) != null;
        boolean enabled = clusterEnabled;
        if (enabled && taskExists == false) {
            startClusterTask();
        } else if (enabled == false && taskExists) {
            stopClusterTask();
        }
    }

    private void reconcileProjectTask(ProjectMetadata projectMetadata) {
        ProjectId projectId = projectMetadata.id();
        String taskId = getProjectTaskId(projectId);
        boolean taskExists = PersistentTasksCustomMetadata.getTaskWithId(projectMetadata, taskId) != null;
        boolean enabled = isEnabled(projectId);
        if (enabled && taskExists == false) {
            startProjectTask(projectId, taskId);
        } else if (enabled == false && taskExists) {
            stopProjectTask(projectId, taskId);
        }
    }

    /// Removes tracking entries from [#perProjectEnabled] for projects that no longer exist in the cluster state.
    /// The persistent tasks framework independently handles the actual cancellation of tasks belonging to removed projects
    void cleanupObsoleteProjectTasks(ClusterState state) {
        Set<ProjectId> obsoleteProjectTasks = Sets.difference(perProjectEnabled.keySet(), state.metadata().projects().keySet());
        for (final var projectId : obsoleteProjectTasks) {
            perProjectEnabled.remove(projectId);
        }
    }

    protected TimeValue masterNodeTimeout() {
        return MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
    }

    private void startClusterTask() {
        persistentTasksService.sendClusterStartRequest(
            getTaskName(),
            getTaskName(),
            paramsFactory.get(),
            masterNodeTimeout(),
            ActionListener.wrap(r -> logger.debug("Created [{}] task", getTaskName()), this::handleStartFailure)
        );
    }

    private void stopClusterTask() {
        persistentTasksService.sendClusterRemoveRequest(
            getTaskName(),
            masterNodeTimeout(),
            ActionListener.wrap(r -> logger.debug("Removed [{}] task", getTaskName()), this::handleStopFailure)
        );
    }

    private void startProjectTask(ProjectId projectId, String taskId) {
        persistentTasksService.sendProjectStartRequest(
            projectId,
            taskId,
            getTaskName(),
            paramsFactory.get(),
            masterNodeTimeout(),
            ActionListener.wrap(r -> logger.debug("Created [{}] task for project [{}]", getTaskName(), projectId), this::handleStartFailure)
        );
    }

    protected void stopProjectTask(ProjectId projectId, String taskId) {
        persistentTasksService.sendProjectRemoveRequest(
            projectId,
            taskId,
            masterNodeTimeout(),
            ActionListener.wrap(r -> logger.debug("Removed [{}] task for project [{}]", getTaskName(), projectId), this::handleStopFailure)
        );
    }

    private void handleStartFailure(Exception e) {
        Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
        if (t instanceof ResourceAlreadyExistsException == false) {
            logger.warn(() -> "Failed to create [" + getTaskName() + "] task", e);
        }
    }

    protected void handleStopFailure(Exception e) {
        Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
        if (t instanceof ResourceNotFoundException == false) {
            logger.warn(() -> "Failed to remove [" + getTaskName() + "] task", e);
        }
    }
}
