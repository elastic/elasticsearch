/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.node.selection.HealthNode.TASK_NAME;

/// [PersistentTasksExecutor] responsible for the lifecycle of the [HealthNode] persistent task.
///
/// On every cluster state change the master node reconciles the desired state: when the feature is enabled
/// (see [ENABLED_SETTING][#ENABLED_SETTING]) and no [HealthNode] task exists yet, one is created; when disabled,
/// any existing task is removed. Non-master nodes ignore the event.
///
/// Once the persistent task framework assigns the task to a node, that node becomes the health node. The
/// [LocalHealthMonitor][org.elasticsearch.health.node.LocalHealthMonitor] and
/// [HealthInfoCache][org.elasticsearch.health.node.HealthInfoCache] use [HealthNode#findHealthNode]
/// to discover it.
///
/// @see HealthNode
/// @see org.elasticsearch.health.node.LocalHealthMonitor
/// @see org.elasticsearch.health.metadata.HealthMetadataService
///
public final class HealthNodeTaskExecutor extends PersistentTasksExecutor<HealthNodeTaskParams> implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(HealthNodeTaskExecutor.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.node.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private volatile boolean enabled;

    private HealthNodeTaskExecutor(ClusterService clusterService, PersistentTasksService persistentTasksService, Settings settings) {
        super(TASK_NAME, clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.enabled = ENABLED_SETTING.get(settings);
    }

    @Override
    public Scope scope() {
        return Scope.CLUSTER;
    }

    public static HealthNodeTaskExecutor create(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        HealthNodeTaskExecutor healthNodeTaskExecutor = new HealthNodeTaskExecutor(clusterService, persistentTasksService, settings);
        healthNodeTaskExecutor.registerListeners(clusterSettings);
        return healthNodeTaskExecutor;
    }

    private void registerListeners(ClusterSettings clusterSettings) {
        clusterService.addListener(this);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, enabled -> this.enabled = enabled);
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, HealthNodeTaskParams params, PersistentTaskState state) {
        DiscoveryNode node = clusterService.localNode();
        logger.info("Node [{}] is selected as the current health node.", node.getShortNodeDescription());
    }

    @Override
    protected HealthNode createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<HealthNodeTaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        return new HealthNode(id, type, action, getDescription(taskInProgress), parentTaskId, headers);
    }

    /// Reconciles the health node task with the desired state on every cluster state update.
    /// Only the master node triggers the lifecycle update. Other nodes return early.
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().clusterRecovered() == false) {
            return;
        }
        final var healthTask = HealthNode.findTask(event.state());
        if (enabled && healthTask == null) {
            persistentTasksService.sendClusterStartRequest(
                TASK_NAME,
                TASK_NAME,
                new HealthNodeTaskParams(),
                TimeValue.THIRTY_SECONDS /* TODO should this be configurable? longer by default? infinite? */,
                ActionListener.wrap(r -> logger.debug("Created the health node task"), e -> {
                    if (e instanceof NodeClosedException) {
                        logger.debug("Failed to create health node task because node is shutting down", e);
                        return;
                    }
                    Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                    if (t instanceof ResourceAlreadyExistsException == false) {
                        logger.error("Failed to create the health node task", e);
                    }
                })
            );
        } else if (enabled == false && healthTask != null) {
            persistentTasksService.sendClusterRemoveRequest(
                TASK_NAME,
                TimeValue.THIRTY_SECONDS,
                ActionListener.wrap(r -> logger.debug("Removed the health node task"), e -> {
                    if (e instanceof NodeClosedException) {
                        logger.debug("Failed to remove health node task because node is shutting down", e);
                        return;
                    }
                    Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                    if (t instanceof ResourceNotFoundException == false) {
                        logger.error("Failed to remove the health node task", e);
                    }
                })
            );
        }
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TASK_NAME), HealthNodeTaskParams::fromXContent)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TASK_NAME, HealthNodeTaskParams::new));
    }
}
