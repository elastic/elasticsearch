/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.ToggleablePersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.node.selection.HealthNode.TASK_NAME;

/// A [ToggleablePersistentTasksExecutor], enabled/disabled via the [ENABLED_SETTING][#ENABLED_SETTING]
/// and responsible for the lifecycle of the [HealthNode] persistent task.
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
public final class HealthNodeTaskExecutor extends ToggleablePersistentTasksExecutor<HealthNodeTaskParams> {

    private static final Logger logger = LogManager.getLogger(HealthNodeTaskExecutor.class);
    private final ClusterService clusterService;

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.node.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public HealthNodeTaskExecutor(ClusterService clusterService, PersistentTasksService persistentTasksService) {
        super(
            TASK_NAME,
            clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT),
            clusterService,
            persistentTasksService,
            ENABLED_SETTING.get(clusterService.getSettings()),
            HealthNodeTaskParams::new
        );
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    public Scope scope() {
        return Scope.CLUSTER;
    }

    @Override
    protected TimeValue masterNodeTimeout() {
        return TimeValue.THIRTY_SECONDS; /* TODO should this be configurable? longer by default? infinite? */
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

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TASK_NAME), HealthNodeTaskParams::fromXContent)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TASK_NAME, HealthNodeTaskParams::new));
    }
}
