/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class TransformNodes {

    private TransformNodes() {}

    /**
     * Get the list of nodes transforms are executing on
     *
     * @param transformIds The transforms.
     * @param clusterState State
     * @return The executor nodes
     */
    public static TransformNodeAssignments transformTaskNodes(List<String> transformIds, ClusterState clusterState) {

        Set<String> executorNodes = new HashSet<>();
        Set<String> assigned = new HashSet<>();
        Set<String> waitingForAssignment = new HashSet<>();

        PersistentTasksCustomMetadata tasksMetadata = PersistentTasksCustomMetadata.getPersistentTasksCustomMetadata(clusterState);

        if (tasksMetadata != null) {
            Set<String> transformIdsSet = new HashSet<>(transformIds);

            Collection<PersistentTasksCustomMetadata.PersistentTask<?>> tasks = tasksMetadata.findTasks(
                TransformField.TASK_NAME,
                t -> transformIdsSet.contains(t.getId())
            );

            for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasks) {
                if (task.isAssigned()) {
                    executorNodes.add(task.getExecutorNode());
                    assigned.add(task.getId());
                } else {
                    waitingForAssignment.add(task.getId());
                }
            }
        }

        Set<String> stopped = transformIds.stream()
            .filter(id -> (assigned.contains(id) || waitingForAssignment.contains(id)) == false)
            .collect(Collectors.toSet());

        return new TransformNodeAssignments(executorNodes, assigned, waitingForAssignment, stopped);
    }
}
