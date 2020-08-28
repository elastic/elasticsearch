/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class TransformNodes {

    private TransformNodes() {
    }

    /**
     * Get the list of nodes transforms are executing on
     *
     * @param transformIds The transforms.
     * @param clusterState State
     * @return The executor nodes
     */
    public static String[] transformTaskNodes(List<String> transformIds, ClusterState clusterState) {

        Set<String> executorNodes = new HashSet<>();

        PersistentTasksCustomMetadata tasksMetadata =
                PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);

        if (tasksMetadata != null) {
            Set<String> transformIdsSet = new HashSet<>(transformIds);

            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> tasks =
                    tasksMetadata.findTasks(TransformField.TASK_NAME, t -> transformIdsSet.contains(t.getId()));

            for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasks) {
                executorNodes.add(task.getExecutorNode());
            }
        }

        return executorNodes.toArray(new String[0]);
    }
}
