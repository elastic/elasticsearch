/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
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

        PersistentTasksCustomMetaData tasksMetaData =
                PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterState);

        if (tasksMetaData != null) {
            Set<String> transformIdsSet = new HashSet<>(transformIds);

            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> tasks =
                    tasksMetaData.findTasks(TransformField.TASK_NAME, t -> transformIdsSet.contains(t.getId()));

            for (PersistentTasksCustomMetaData.PersistentTask<?> task : tasks) {
                executorNodes.add(task.getExecutorNode());
            }
        }

        return executorNodes.toArray(new String[0]);
    }
}
