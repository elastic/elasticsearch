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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.node.selection.HealthNode.TASK_NAME;

/// [PersistentTasksExecutor] responsible for the lifecycle of the [HealthNode] persistent task.
///
/// The task lifecycle (start / stop) is managed externally by the [PersistentTaskLifecycleManager],
/// which reconciles the desired state on every cluster state update based on [HealthNodeTaskExecutor#ENABLED_SETTING].
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
public final class HealthNodeTaskExecutor extends PersistentTasksExecutor<HealthNodeTaskParams> {

    private static final Logger logger = LogManager.getLogger(HealthNodeTaskExecutor.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "health.node.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;

    public HealthNodeTaskExecutor(ClusterService clusterService) {
        super(TASK_NAME, clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
    }

    @Override
    public Scope scope() {
        return Scope.CLUSTER;
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
