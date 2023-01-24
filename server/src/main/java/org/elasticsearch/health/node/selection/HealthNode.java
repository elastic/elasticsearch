/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * Main component used for selecting the health node of the cluster
 */
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
        PersistentTasksCustomMetadata taskMetadata = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        return taskMetadata == null ? null : taskMetadata.getTask(TASK_NAME);
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
