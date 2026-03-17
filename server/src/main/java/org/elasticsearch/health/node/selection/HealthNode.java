/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/// A persistent task that designates a single node in the cluster as the "health node". The health node is
/// responsible for aggregating per-node health information reported by
/// [LocalHealthMonitor][org.elasticsearch.health.node.LocalHealthMonitor] instances running on every node:
///
/// - [DiskHealthInfo][org.elasticsearch.health.node.DiskHealthInfo]
/// - [RepositoriesHealthInfo][org.elasticsearch.health.node.RepositoriesHealthInfo]
/// - [DataStreamLifecycleHealthInfo][org.elasticsearch.health.node.DataStreamLifecycleHealthInfo]
/// - [FileSettingsHealthInfo][org.elasticsearch.health.node.FileSettingsHealthInfo]
///
/// The aggregated data is held in the [HealthInfoCache][org.elasticsearch.health.node.HealthInfoCache] on the
/// health node and consumed by health indicator services.
///
/// The health node also logs nodes health periodically via the
/// [HealthPeriodicLogger][org.elasticsearch.health.HealthPeriodicLogger].
///
/// The lifecycle of this task is managed by [HealthNodeTaskExecutor].
///
/// @see HealthNodeTaskExecutor
/// @see org.elasticsearch.health.node.HealthInfoCache
/// @see org.elasticsearch.health.node.LocalHealthMonitor
/// @see org.elasticsearch.health.HealthPeriodicLogger
///
public class HealthNode extends AllocatedPersistentTask {

    public static final String TASK_NAME = "health-node";

    HealthNode(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    @Override
    protected void onCancelled() {
        markAsCompleted();
    }

    @Nullable
    public static PersistentTasksCustomMetadata.PersistentTask<?> findTask(ClusterState clusterState) {
        return ClusterPersistentTasksCustomMetadata.getTaskWithId(clusterState, TASK_NAME);
    }

    @Nullable
    public static DiscoveryNode findHealthNode(ClusterState clusterState) {
        PersistentTasksCustomMetadata.PersistentTask<?> task = findTask(clusterState);
        if (task == null || task.isAssigned() == false) {
            return null;
        }
        return clusterState.nodes().get(task.getAssignment().getExecutorNode());
    }
}
