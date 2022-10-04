/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealthIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

import java.util.List;

/**
 *
 */
public final class TransformHealthChecker {

    public static TransformHealth checkUnassignedTransform(String transformId, ClusterState clusterState) {
        final Assignment assignment = TransformNodes.getAssignment(transformId, clusterState);
        return new TransformHealth(
            HealthStatus.RED,
            List.of(new TransformHealthIssue("Failed to assign transform to a node", assignment.getExplanation()))
        );
    }

    public static TransformHealth checkTransform(TransformTask transformtask) {
        if (TransformTaskState.FAILED.equals(transformtask.getState().getTaskState())) {
            return new TransformHealth(
                HealthStatus.RED,
                List.of(new TransformHealthIssue("Transform state is [failed]", transformtask.getState().getReason()))
            );
        }
        return new TransformHealth(HealthStatus.GREEN, null);
    }

    private TransformHealthChecker() {}
}
