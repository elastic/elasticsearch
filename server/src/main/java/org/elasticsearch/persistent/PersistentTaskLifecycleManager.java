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
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
/// Each registration method accepts a [Setting] whose value is watched for dynamic updates.
/// Reconciliation runs independently for every project in the cluster state.
///
/// At most one request is in flight at a time per task (per project for project-scoped tasks). If the
/// enabled state changes while a request is in flight, the opposite request is sent immediately upon
/// completion without waiting for the next cluster state update.
///
/// Tasks controlled by more complex logic (e.g. requiring per-project conditions or custom stop behavior)
/// should have separate classes managing their own lifecycle.
///
public final class PersistentTaskLifecycleManager implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PersistentTaskLifecycleManager.class);

    private enum InFlightRequest {
        NONE,
        START,
        REMOVE,
        STALE
    }

    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final List<ClusterTaskRegistration> clusterRegistrations = new ArrayList<>();
    private final List<ProjectTaskRegistration> projectRegistrations = new ArrayList<>();

    public PersistentTaskLifecycleManager(PersistentTasksService persistentTasksService, ClusterService clusterService) {
        this.persistentTasksService = persistentTasksService;
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
        clusterService.addListener(this);
    }

    /// Registers a cluster-scoped task whose enabled state is driven by a [Setting].
    /// The manager watches the setting for dynamic changes and reconciles accordingly.
    ///
    /// @param taskName       the task name, also used as the task ID in cluster state
    /// @param enabledSetting setting that controls whether the task should be running
    /// @param paramsSupplier called only when a start request is needed
    public void registerClusterTask(
        String taskName,
        Setting<Boolean> enabledSetting,
        Supplier<? extends PersistentTaskParams> paramsSupplier
    ) {
        final var enabled = new AtomicBoolean();
        clusterSettings.initializeAndWatch(enabledSetting, enabled::set);
        registerClusterTask(taskName, enabled::get, paramsSupplier);
    }

    // visible for testing
    void registerClusterTask(String taskName, BooleanSupplier enabled, Supplier<? extends PersistentTaskParams> paramsSupplier) {
        clusterRegistrations.add(new ClusterTaskRegistration(taskName, enabled, paramsSupplier));
    }

    /// Registers a project-scoped task whose enabled state is driven by a [Setting].
    /// The manager watches the setting for dynamic changes and reconciles accordingly.
    ///
    /// @param taskName       the task name, used to identify the executor
    /// @param taskIdFn       maps a [ProjectId] to the task ID stored in that project's task metadata
    /// @param enabledSetting setting that controls whether the task should be running
    /// @param paramsSupplier called only when a start request is needed
    public void registerProjectTask(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        Setting<Boolean> enabledSetting,
        Supplier<? extends PersistentTaskParams> paramsSupplier
    ) {
        final var enabled = new AtomicBoolean();
        clusterSettings.initializeAndWatch(enabledSetting, enabled::set);
        registerProjectTask(taskName, taskIdFn, enabled::get, paramsSupplier);
    }

    // visible for testing
    void registerProjectTask(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier
    ) {
        projectRegistrations.add(new ProjectTaskRegistration(taskName, taskIdFn, enabled, paramsSupplier));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().nodes().getMasterNode() == null || event.state().clusterRecovered() == false) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        if (event.metadataChanged() == false && event.previousState().nodes().isLocalNodeElectedMaster()) {
            return;
        }
        reconcile(event.state());
    }

    private void reconcile(ClusterState state) {
        for (var reg : clusterRegistrations) {
            reconcileClusterTask(state, reg);
        }
        final var projects = state.metadata().projects();
        for (var reg : projectRegistrations) {
            reg.inFlightRequests()
                .entrySet()
                .removeIf(
                    e -> projects.containsKey(e.getKey()) == false
                        && e.getValue().compareAndSet(InFlightRequest.NONE, InFlightRequest.STALE)
                );
            for (var project : projects.values()) {
                reconcileProjectTask(project, reg);
            }
        }
    }

    private void reconcileClusterTask(ClusterState state, ClusterTaskRegistration reg) {
        final boolean taskExists = ClusterPersistentTasksCustomMetadata.getTaskWithId(state, reg.taskName()) != null;
        final boolean enabled = reg.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            sendClusterTaskStartRequest(reg);
        } else if (enabled == false && taskExists) {
            sendClusterTaskRemoveRequest(reg);
        }
    }

    private void sendClusterTaskStartRequest(ClusterTaskRegistration reg) {
        if (reg.inFlightRequest().compareAndSet(InFlightRequest.NONE, InFlightRequest.START) == false) {
            return;
        }
        persistentTasksService.sendClusterStartRequest(
            reg.taskName(),
            reg.taskName(),
            reg.paramsSupplier().get(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> onClusterTaskStartRequestComplete(reg), e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceAlreadyExistsException) {
                    onClusterTaskStartRequestComplete(reg);
                } else {
                    logger.warn(() -> "Failed to create [" + reg.taskName() + "] task", e);
                    onClusterTaskRequestFailure(reg);
                }
            })
        );
    }

    private void sendClusterTaskRemoveRequest(ClusterTaskRegistration reg) {
        if (reg.inFlightRequest().compareAndSet(InFlightRequest.NONE, InFlightRequest.REMOVE) == false) {
            return;
        }
        persistentTasksService.sendClusterRemoveRequest(
            reg.taskName(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> onClusterTaskRemoveRequestComplete(reg), e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceNotFoundException) {
                    onClusterTaskRemoveRequestComplete(reg);
                } else {
                    logger.warn(() -> "Failed to remove [" + reg.taskName() + "] task", e);
                    onClusterTaskRequestFailure(reg);
                }
            })
        );
    }

    private void onClusterTaskRequestFailure(ClusterTaskRegistration reg) {
        reg.inFlightRequest().set(InFlightRequest.NONE);
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        if (reg.enabled().getAsBoolean()) {
            sendClusterTaskStartRequest(reg);
        } else {
            sendClusterTaskRemoveRequest(reg);
        }
    }

    private void onClusterTaskStartRequestComplete(ClusterTaskRegistration reg) {
        logger.debug("Created [{}] task", reg.taskName());
        reg.inFlightRequest().set(InFlightRequest.NONE);
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        if (reg.enabled().getAsBoolean() == false) {
            sendClusterTaskRemoveRequest(reg);
        }
    }

    private void onClusterTaskRemoveRequestComplete(ClusterTaskRegistration reg) {
        logger.debug("Removed [{}] task", reg.taskName());
        reg.inFlightRequest().set(InFlightRequest.NONE);
        if (clusterService.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }
        if (reg.enabled().getAsBoolean()) {
            sendClusterTaskStartRequest(reg);
        }
    }

    private void reconcileProjectTask(ProjectMetadata project, ProjectTaskRegistration reg) {
        final var projectId = project.id();
        reg.inFlightRequests().computeIfAbsent(projectId, k -> new AtomicReference<>(InFlightRequest.NONE));
        final var taskId = reg.taskIdFn().apply(projectId);
        final boolean taskExists = PersistentTasksCustomMetadata.getTaskWithId(project, taskId) != null;
        final boolean enabled = reg.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            sendProjectTaskStartRequest(reg, projectId, taskId);
        } else if (enabled == false && taskExists) {
            sendProjectTaskRemoveRequest(reg, projectId, taskId);
        }
    }

    private void sendProjectTaskStartRequest(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        if (reg.inFlightRequests().get(projectId).compareAndSet(InFlightRequest.NONE, InFlightRequest.START) == false) {
            return;
        }
        persistentTasksService.sendProjectStartRequest(
            projectId,
            taskId,
            reg.taskName(),
            reg.paramsSupplier().get(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> onProjectTaskStartRequestComplete(reg, projectId, taskId), e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceAlreadyExistsException) {
                    onProjectTaskStartRequestComplete(reg, projectId, taskId);
                } else {
                    logger.warn(() -> "Failed to create [" + reg.taskName() + "] task for project [" + projectId + "]", e);
                    onProjectTaskRequestFailure(reg, projectId, taskId);
                }
            })
        );
    }

    private void sendProjectTaskRemoveRequest(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        if (reg.inFlightRequests().get(projectId).compareAndSet(InFlightRequest.NONE, InFlightRequest.REMOVE) == false) {
            return;
        }
        persistentTasksService.sendProjectRemoveRequest(
            projectId,
            taskId,
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(r -> onProjectTaskRemoveRequestComplete(reg, projectId, taskId), e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceNotFoundException) {
                    onProjectTaskRemoveRequestComplete(reg, projectId, taskId);
                } else {
                    logger.warn(() -> "Failed to remove [" + reg.taskName() + "] task for project [" + projectId + "]", e);
                    onProjectTaskRequestFailure(reg, projectId, taskId);
                }
            })
        );
    }

    private void onProjectTaskRequestFailure(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        reg.inFlightRequests().get(projectId).set(InFlightRequest.NONE);
        final var state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false || state.metadata().projects().containsKey(projectId) == false) {
            return;
        }
        if (reg.enabled().getAsBoolean()) {
            sendProjectTaskStartRequest(reg, projectId, taskId);
        } else {
            sendProjectTaskRemoveRequest(reg, projectId, taskId);
        }
    }

    private void onProjectTaskStartRequestComplete(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        logger.debug("Created [{}] task for project [{}]", reg.taskName(), projectId);
        reg.inFlightRequests().get(projectId).set(InFlightRequest.NONE);
        final var state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false || state.metadata().projects().containsKey(projectId) == false) {
            return;
        }
        if (reg.enabled().getAsBoolean() == false) {
            sendProjectTaskRemoveRequest(reg, projectId, taskId);
        }
    }

    private void onProjectTaskRemoveRequestComplete(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        logger.debug("Removed [{}] task for project [{}]", reg.taskName(), projectId);
        reg.inFlightRequests().get(projectId).set(InFlightRequest.NONE);
        final var state = clusterService.state();
        if (state.nodes().isLocalNodeElectedMaster() == false || state.metadata().projects().containsKey(projectId) == false) {
            return;
        }
        if (reg.enabled().getAsBoolean()) {
            sendProjectTaskStartRequest(reg, projectId, taskId);
        }
    }

    private record ClusterTaskRegistration(
        String taskName,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        AtomicReference<InFlightRequest> inFlightRequest
    ) {
        ClusterTaskRegistration(String taskName, BooleanSupplier enabled, Supplier<? extends PersistentTaskParams> paramsSupplier) {
            this(taskName, enabled, paramsSupplier, new AtomicReference<>(InFlightRequest.NONE));
        }
    }

    private record ProjectTaskRegistration(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        ConcurrentHashMap<ProjectId, AtomicReference<InFlightRequest>> inFlightRequests
    ) {
        ProjectTaskRegistration(
            String taskName,
            Function<ProjectId, String> taskIdFn,
            BooleanSupplier enabled,
            Supplier<? extends PersistentTaskParams> paramsSupplier
        ) {
            this(taskName, taskIdFn, enabled, paramsSupplier, new ConcurrentHashMap<>());
        }
    }
}
