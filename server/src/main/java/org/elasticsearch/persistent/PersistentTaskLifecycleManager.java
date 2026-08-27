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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.NodeClosedException;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
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
public final class PersistentTaskLifecycleManager extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(PersistentTaskLifecycleManager.class);

    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;
    private final List<ClusterTaskRegistration> clusterRegistrations = new CopyOnWriteArrayList<>();
    private final List<ProjectTaskRegistration> projectRegistrations = new CopyOnWriteArrayList<>();

    public PersistentTaskLifecycleManager(PersistentTasksService persistentTasksService, ClusterService clusterService) {
        this.persistentTasksService = persistentTasksService;
        this.clusterService = clusterService;
        this.clusterSettings = clusterService.getClusterSettings();
    }

    @Override
    protected void doStart() {
        clusterService.addListener(this);
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() {}

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
        clusterRegistrations.add(new ClusterTaskRegistration(taskName, enabled::get, paramsSupplier));
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
        registerProjectTask(taskName, taskIdFn, enabledSetting, paramsSupplier, p -> {});
    }

    /// Registers a project-scoped task whose enabled state is driven by a [Setting].
    /// The manager watches the setting for dynamic changes and reconciles accordingly.
    ///
    /// @param taskName       the task name, used to identify the executor
    /// @param taskIdFn       maps a [ProjectId] to the task ID stored in that project's task metadata
    /// @param enabledSetting setting that controls whether the task should be running
    /// @param paramsSupplier called only when a start request is needed
    /// @param onRemove       called with the [ProjectId] after the task is confirmed removed (on success or [ResourceNotFoundException])
    @FixForMultiProject(description = "When settings become project-scoped, clusterSettings.initializeAndWatch won't work")
    public void registerProjectTask(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        Setting<Boolean> enabledSetting,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        Consumer<ProjectId> onRemove
    ) {
        final var enabled = new AtomicBoolean();
        clusterSettings.initializeAndWatch(enabledSetting, enabled::set);
        projectRegistrations.add(new ProjectTaskRegistration(taskName, taskIdFn, enabled::get, paramsSupplier, onRemove));
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }
        if (event.metadataChanged() == false && event.masterChanged() == false && event.clusterJustRecovered() == false) {
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
            for (var projectId : projects.keySet()) {
                reconcileProjectTask(state, reg, projectId);
            }
        }
    }

    private void reconcileClusterTask(ClusterState state, ClusterTaskRegistration reg) {
        if (lifecycle.started() == false) {
            return;
        }
        if (state.nodes().isLocalNodeElectedMaster() == false || state.clusterRecovered() == false) {
            return;
        }
        final boolean taskExists = ClusterPersistentTasksCustomMetadata.getTaskWithId(state, reg.taskName()) != null;
        final boolean enabled = reg.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            sendClusterTaskStartRequest(reg);
        } else if (enabled == false && taskExists) {
            sendClusterTaskRemoveRequest(reg);
        } else {
            logger.trace("Reconciliation of [{}] task complete", reg.taskName());
        }
    }

    private void sendClusterTaskStartRequest(ClusterTaskRegistration reg) {
        if (reg.inFlight().compareAndSet(false, true) == false) {
            return;
        }
        persistentTasksService.sendClusterStartRequest(
            reg.taskName(),
            reg.taskName(),
            reg.paramsSupplier().get(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(ActionListener.wrap(r -> {
                logger.debug("Created [{}] task", reg.taskName());
            }, e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof NodeClosedException == false && t instanceof ResourceAlreadyExistsException == false) {
                    logger.warn(() -> "Failed to create [" + reg.taskName() + "] task", e);
                }
            }), () -> {
                reg.inFlight().set(false);
                reconcileClusterTask(clusterService.state(), reg);
            })
        );
    }

    private void sendClusterTaskRemoveRequest(ClusterTaskRegistration reg) {
        if (reg.inFlight().compareAndSet(false, true) == false) {
            return;
        }
        persistentTasksService.sendClusterRemoveRequest(
            reg.taskName(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(ActionListener.wrap(r -> {
                logger.debug("Removed [{}] task", reg.taskName());
            }, e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof NodeClosedException == false && t instanceof ResourceNotFoundException == false) {
                    logger.warn(() -> "Failed to remove [" + reg.taskName() + "] task", e);
                }
            }), () -> {
                reg.inFlight().set(false);
                reconcileClusterTask(clusterService.state(), reg);
            })
        );
    }

    private void reconcileProjectTask(ClusterState state, ProjectTaskRegistration registration, ProjectId projectId) {
        if (lifecycle.started() == false) {
            return;
        }
        if (state.nodes().isLocalNodeElectedMaster() == false || state.clusterRecovered() == false) {
            return;
        }
        final var project = state.metadata().projects().get(projectId);
        if (project == null) {
            return;
        }
        final var taskId = registration.taskIdFn().apply(projectId);
        final boolean taskExists = PersistentTasksCustomMetadata.getTaskWithId(project, taskId) != null;
        final boolean enabled = registration.enabled().getAsBoolean();
        if (enabled && taskExists == false) {
            sendProjectTaskStartRequest(registration, projectId, taskId);
        } else if (enabled == false && taskExists) {
            sendProjectTaskRemoveRequest(registration, projectId, taskId);
        } else {
            logger.trace("Reconciliation of [{}] task in project [{}] complete", registration.taskName(), projectId);
        }
    }

    private void sendProjectTaskStartRequest(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        if (reg.inFlightRequests().add(projectId) == false) {
            return;
        }
        persistentTasksService.sendProjectStartRequest(
            projectId,
            taskId,
            reg.taskName(),
            reg.paramsSupplier().get(),
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(ActionListener.wrap(r -> {
                logger.debug("Created [{}] task for project [{}]", reg.taskName(), projectId);
            }, e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof NodeClosedException == false && t instanceof ResourceAlreadyExistsException == false) {
                    logger.warn(() -> "Failed to create [" + reg.taskName() + "] task for project [" + projectId + "]", e);
                }
            }), () -> {
                reg.inFlightRequests().remove(projectId);
                reconcileProjectTask(clusterService.state(), reg, projectId);
            })
        );
    }

    private void sendProjectTaskRemoveRequest(ProjectTaskRegistration reg, ProjectId projectId, String taskId) {
        if (reg.inFlightRequests().add(projectId) == false) {
            return;
        }
        persistentTasksService.sendProjectRemoveRequest(
            projectId,
            taskId,
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            ActionListener.runAfter(ActionListener.wrap(r -> {
                logger.debug("Removed [{}] task for project [{}]", reg.taskName(), projectId);
                reg.onRemove().accept(projectId);
            }, e -> {
                final var t = ExceptionsHelper.unwrapCause(e);
                if (t instanceof ResourceNotFoundException) {
                    reg.onRemove().accept(projectId);
                } else if (t instanceof NodeClosedException == false) {
                    logger.warn(() -> "Failed to remove [" + reg.taskName() + "] task for project [" + projectId + "]", e);
                }
            }), () -> {
                reg.inFlightRequests().remove(projectId);
                reconcileProjectTask(clusterService.state(), reg, projectId);
            })
        );
    }

    private record ClusterTaskRegistration(
        String taskName,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        AtomicBoolean inFlight
    ) {
        ClusterTaskRegistration(String taskName, BooleanSupplier enabled, Supplier<? extends PersistentTaskParams> paramsSupplier) {
            this(taskName, enabled, paramsSupplier, new AtomicBoolean());
        }
    }

    private record ProjectTaskRegistration(
        String taskName,
        Function<ProjectId, String> taskIdFn,
        BooleanSupplier enabled,
        Supplier<? extends PersistentTaskParams> paramsSupplier,
        Consumer<ProjectId> onRemove,
        Set<ProjectId> inFlightRequests
    ) {
        ProjectTaskRegistration(
            String taskName,
            Function<ProjectId, String> taskIdFn,
            BooleanSupplier enabled,
            Supplier<? extends PersistentTaskParams> paramsSupplier,
            Consumer<ProjectId> onRemove
        ) {
            this(taskName, taskIdFn, enabled, paramsSupplier, onRemove, ConcurrentHashMap.newKeySet());
        }
    }
}
