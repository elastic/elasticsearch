/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealthIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

import java.util.ArrayList;
import java.util.List;

/**
 * Check the health of a transform.
 */
public final class TransformHealthChecker {

    // simple boundary to decide when to report a red status vs. a yellow status after consecutive retries
    static int RED_STATUS_FAILURE_COUNT_BOUNDARY = 5;

    public static TransformHealth checkUnassignedTransform(String transformId, ClusterState clusterState) {
        final Assignment assignment = TransformNodes.getAssignment(transformId, clusterState);
        return new TransformHealth(
            HealthStatus.RED,
            List.of(new TransformHealthIssue("Failed to assign transform to a node", assignment.getExplanation(), 1, null))
        );
    }

    public static TransformHealth checkTransform(TransformTask transformTask) {
        // quick check
        if (TransformTaskState.FAILED.equals(transformTask.getState().getTaskState()) == false
            && transformTask.getContext().getFailureCount() == 0
            && transformTask.getContext().getStatePersistenceFailureCount() == 0) {
            return TransformHealth.GREEN;
        }

        final TransformContext transformContext = transformTask.getContext();
        List<TransformHealthIssue> issues = new ArrayList<>();
        HealthStatus maxStatus = HealthStatus.GREEN;

        if (TransformTaskState.FAILED.equals(transformTask.getState().getTaskState())) {
            maxStatus = HealthStatus.RED;
            issues.add(
                new TransformHealthIssue(
                    "Transform task state is [failed]",
                    transformTask.getState().getReason(),
                    1,
                    transformContext.getStateFailureTime()
                )
            );
        }

        if (transformContext.getFailureCount() != 0) {
            final Throwable lastFailure = transformContext.getLastFailure();
            final String lastFailureMessage = lastFailure instanceof ElasticsearchException elasticsearchException
                ? elasticsearchException.getDetailedMessage()
                : lastFailure.getMessage();

            if (HealthStatus.RED.equals(maxStatus) == false) {
                maxStatus = transformContext.getFailureCount() > RED_STATUS_FAILURE_COUNT_BOUNDARY ? HealthStatus.RED : HealthStatus.YELLOW;
            }

            issues.add(
                new TransformHealthIssue(
                    "Transform indexer failed",
                    lastFailureMessage,
                    transformContext.getFailureCount(),
                    transformContext.getLastFailureStartTime()
                )
            );
        }

        if (transformContext.getStatePersistenceFailureCount() != 0) {
            if (HealthStatus.RED.equals(maxStatus) == false) {
                maxStatus = transformContext.getStatePersistenceFailureCount() > RED_STATUS_FAILURE_COUNT_BOUNDARY
                    ? HealthStatus.RED
                    : HealthStatus.YELLOW;
            }

            issues.add(
                new TransformHealthIssue(
                    "Task encountered failures updating internal state",
                    transformContext.getLastStatePersistenceFailure().getMessage(),
                    transformContext.getStatePersistenceFailureCount(),
                    transformContext.getLastStatePersistenceFailureStartTime()
                )
            );
        }

        return new TransformHealth(maxStatus, issues);
    }

    private TransformHealthChecker() {}
}
