/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class DataFrameNodes {

    private DataFrameNodes() {
    }

    /**
     * Get the list of nodes the data frames are executing on
     *
     * @param dataFrameIds The data frames.
     * @param clusterState State
     * @return The executor nodes
     */
    public static String[] dataFrameTaskNodes(List<String> dataFrameIds, ClusterState clusterState) {

        Set<String> executorNodes = new HashSet<>();

        PersistentTasksCustomMetaData tasksMetaData =
                PersistentTasksCustomMetaData.getPersistentTasksCustomMetaData(clusterState);

        if (tasksMetaData != null) {
            Set<String> dataFrameIdsSet = new HashSet<>(dataFrameIds);

            Collection<PersistentTasksCustomMetaData.PersistentTask<?>> tasks =
                    tasksMetaData.findTasks(DataFrameField.TASK_NAME, t -> dataFrameIdsSet.contains(t.getId()));

            for (PersistentTasksCustomMetaData.PersistentTask<?> task : tasks) {
                executorNodes.add(task.getExecutorNode());
            }
        }

        return executorNodes.toArray(new String[0]);
    }
}
