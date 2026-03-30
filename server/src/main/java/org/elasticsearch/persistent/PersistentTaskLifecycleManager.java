/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

/// Manages the lifecycle of persistent tasks that are controlled by a [BooleanSupplier] enabled flag.
///
/// On every cluster state update, the master node reconciles the desired state (enabled / disabled) with the
/// actual state (task exists / does not exist) and sends the appropriate start or remove request through
/// the [PersistentTasksService].
///
/// Both cluster-scoped and project-scoped tasks are supported. Cluster tasks can be registered via
/// [#registerClusterTask], and project tasks via [#registerProjectTask].
/// Each registration method has two variants: one accepting a [BooleanSupplier] for arbitrary enabled logic,
/// and one accepting a [Setting] whose value is watched for dynamic updates.
/// Reconciliation runs independently for every project in the cluster state.
///
/// Tasks controlled by more complex logic (e.g. requiring per-project conditions or custom stop behavior)
/// have separate classes managing their own lifecycle.
///
public final class PersistentTaskLifecycleManager implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PersistentTaskLifecycleManager.class);

    private final PersistentTasksService persistentTasksService;
    private final ClusterSettings clusterSettings;
    private final List<ClusterTaskRegistration> clusterRegistrations = new ArrayList<>();
    private final List<ProjectTaskRegistration> projectRegistrations = new ArrayList<>();

    public PersistentTaskLifecycleManager(PersistentTasksService persistentTasksService, ClusterService clusterService) {
        this.persistentTasksService = persistentTasksService;
        this.clusterSettings = clusterService.getClusterSettings();
        clusterService.addListener(this);
    }

    /// Registers a cluster-scoped task.
    ///
    /// @param taskName         the task name, also used as the task ID in cluster state
    /// @param enabled          called on each cluster state update to determine whether the task should be running
    /// @param paramsSupplier   called only when a start request is needed
    /// @param masterNodeTimeout timeout for start/remove requests sent to the master
    public void registerClusterTask(
        String taskName,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {
        clusterRegistrations.add(new ClusterTaskRegistration(taskName, enabled, paramsSupplier, masterNodeTimeout));
    }

    /// Registers a cluster-scoped task whose enabled state is driven by a [Setting].
    /// The manager watches the setting for dynamic changes and reconciles accordingly.
    ///
    /// @param taskName         the task name, also used as the task ID in cluster state
    /// @param enabledSetting   setting that controls whether the task should be running
    /// @param paramsSupplier   called only when a start request is needed
    /// @param masterNodeTimeout timeout for start/remove requests sent to the master
    public void registerClusterTask(
        String taskName,
        Setting<Boolean> enabledSetting,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {
        final var enabled = new AtomicBoolean();
        clusterSettings.initializeAndWatch(enabledSetting, enabled::set);
        registerClusterTask(taskName, enabled::get, paramsSupplier, masterNodeTimeout);
    }

    /// Registers a project-scoped task.
    /// The task is reconciled independently for every project present in the cluster state.
    ///
    /// @param taskName         the task name, used to identify the executor
    /// @param taskIdFn         maps a [ProjectId] to the task ID stored in that project's task metadata
    /// @param enabled          called on each reconciliation to determine whether the task should be running
    /// @param paramsSupplier   called only when a start request is needed
    /// @param masterNodeTimeout timeout for start/remove requests sent to the master
    public void registerProjectTask(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {
        projectRegistrations.add(new ProjectTaskRegistration(taskName, taskIdFn, enabled, paramsSupplier, masterNodeTimeout));
    }

    /// Registers a project-scoped task whose enabled state is driven by a [Setting].
    /// The manager watches the setting for dynamic changes and reconciles accordingly.
    ///
    /// @param taskName         the task name, used to identify the executor
    /// @param taskIdFn         maps a [ProjectId] to the task ID stored in that project's task metadata
    /// @param enabledSetting   setting that controls whether the task should be running
    /// @param paramsSupplier   called only when a start request is needed
    /// @param masterNodeTimeout timeout for start/remove requests sent to the master
    public void registerProjectTask(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        Setting<Boolean> enabledSetting,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {
        final var enabled = new AtomicBoolean();
        clusterSettings.initializeAndWatch(enabledSetting, enabled::set);
        registerProjectTask(taskName, taskIdFn, enabled::get, paramsSupplier, masterNodeTimeout);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().getMasterNode() == null || event.state().clusterRecovered() == false) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        reconcile(event.state());
    }

    private void reconcile(ClusterState state) {
        for (var reg : clusterRegistrations) {
            reconcileClusterTask(state, reg);
        }
        for (var reg : projectRegistrations) {
            for (var project : state.metadata().projects().values()) {
                reconcileProjectTask(project, reg);
            }
        }
    }

    private void reconcileClusterTask(ClusterState state, ClusterTaskRegistration reg) {
        final boolean taskExists = ClusterPersistentTasksCustomMetadata.getTaskWithId(state, reg.taskName()) != null;
        final boolean enabled = reg.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            persistentTasksService.sendClusterStartRequest(
                reg.taskName(),
                reg.taskName(),
                reg.paramsSupplier().get(),
                reg.masterNodeTimeout(),
                ActionListener.wrap(
                    r -> logger.debug("Created [{}] task", reg.taskName()),
                    e -> handleStartFailure(reg.taskName(), null, e)
                )
            );
        } else if (enabled == false && taskExists) {
            persistentTasksService.sendClusterRemoveRequest(
                reg.taskName(),
                reg.masterNodeTimeout(),
                ActionListener.wrap(r -> logger.debug("Removed [{}] task", reg.taskName()), e -> handleStopFailure(reg.taskName(), null, e))
            );
        }
    }

    private void reconcileProjectTask(ProjectMetadata project, ProjectTaskRegistration reg) {
        final var projectId = project.id();
        final var taskId = reg.taskIdFn().apply(projectId);
        final boolean taskExists = PersistentTasksCustomMetadata.getTaskWithId(project, taskId) != null;
        final boolean enabled = reg.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            persistentTasksService.sendProjectStartRequest(
                projectId,
                taskId,
                reg.taskName(),
                reg.paramsSupplier().get(),
                reg.masterNodeTimeout(),
                ActionListener.wrap(
                    r -> logger.debug("Created [{}] task for project [{}]", reg.taskName(), projectId),
                    e -> handleStartFailure(reg.taskName(), projectId, e)
                )
            );
        } else if (enabled == false && taskExists) {
            persistentTasksService.sendProjectRemoveRequest(
                projectId,
                taskId,
                reg.masterNodeTimeout(),
                ActionListener.wrap(
                    r -> logger.debug("Removed [{}] task for project [{}]", reg.taskName(), projectId),
                    e -> handleStopFailure(reg.taskName(), projectId, e)
                )
            );
        }
    }

    private void handleStartFailure(String taskName, @Nullable ProjectId projectId, Exception e) {
        Throwable t = ExceptionsHelper.unwrapCause(e);
        if (t instanceof ResourceAlreadyExistsException == false) {
            if (projectId == null) {
                logger.warn(() -> "Failed to create [" + taskName + "] task", e);
            } else {
                logger.warn(() -> "Failed to create [" + taskName + "] task for project [" + projectId + "]", e);
            }
        }
    }

    private void handleStopFailure(String taskName, @Nullable ProjectId projectId, Exception e) {
        Throwable t = ExceptionsHelper.unwrapCause(e);
        if (t instanceof ResourceNotFoundException == false) {
            if (projectId == null) {
                logger.warn(() -> "Failed to remove [" + taskName + "] task", e);
            } else {
                logger.warn(() -> "Failed to remove [" + taskName + "] task for project [" + projectId + "]", e);
            }
        }
    }

    private record ClusterTaskRegistration(
        String taskName,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {}

    private record ProjectTaskRegistration(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        TimeValue masterNodeTimeout
    ) {}
}
