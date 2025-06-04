/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealth;
import org.elasticsearch.xpack.core.transform.transforms.TransformHealthIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Check the health of a transform.
 */
public final class TransformHealthChecker {

    /**
     * Describes all the known transform health issue types.
     *
     * The list of issue type can be extended over time, when we add additional health checks.
     */
    public enum IssueType {
        ASSIGNMENT_FAILED("Failed to assign transform to a node"),
        PRIVILEGES_CHECK_FAILED("Privileges check failed"),
        TRANSFORM_TASK_FAILED("Transform task state is [failed]"),
        TRANSFORM_INDEXER_FAILED("Transform indexer failed"),
        TRANSFORM_INTERNAL_STATE_UPDATE_FAILED("Task encountered failures updating internal state"),
        TRANSFORM_STARTUP_FAILED("Transform task is automatically retrying its startup process");

        private final String issue;

        IssueType(String issue) {
            this.issue = issue;
        }

        public TransformHealthIssue newIssue(@Nullable String details, int count, @Nullable Instant firstOccurrence) {
            String type = name().toLowerCase(Locale.ROOT);
            return new TransformHealthIssue(type, issue, details, count, firstOccurrence);
        }
    }

    // simple boundary to decide when to report a red status vs. a yellow status after consecutive retries
    static int RED_STATUS_FAILURE_COUNT_BOUNDARY = 5;

    public static TransformHealth checkUnassignedTransform(
        String transformId,
        ClusterState clusterState,
        @Nullable AuthorizationState authState
    ) {
        final Assignment assignment = TransformNodes.getAssignment(transformId, clusterState);
        final List<TransformHealthIssue> issues = new ArrayList<>(2);
        issues.add(IssueType.ASSIGNMENT_FAILED.newIssue(assignment.getExplanation(), 1, null));
        if (AuthorizationState.isNullOrGreen(authState) == false) {
            issues.add(IssueType.PRIVILEGES_CHECK_FAILED.newIssue(authState.getLastAuthError(), 1, null));
        }
        return new TransformHealth(HealthStatus.RED, Collections.unmodifiableList(issues));
    }

    public static TransformHealth checkTransform(@Nullable AuthorizationState authState) {
        // quick check
        if (AuthorizationState.isNullOrGreen(authState)) {
            return TransformHealth.GREEN;
        }

        return new TransformHealth(
            authState.getStatus(),
            List.of(IssueType.PRIVILEGES_CHECK_FAILED.newIssue(authState.getLastAuthError(), 1, null))
        );
    }

    public static TransformHealth checkTransform(TransformTask transformTask) {
        return checkTransform(transformTask, null);
    }

    public static TransformHealth checkTransform(TransformTask transformTask, @Nullable AuthorizationState authState) {
        // quick check
        if (TransformTaskState.FAILED.equals(transformTask.getState().getTaskState()) == false
            && transformTask.getContext().doesNotHaveFailures()
            && AuthorizationState.isNullOrGreen(authState)) {
            return TransformHealth.GREEN;
        }

        final TransformContext transformContext = transformTask.getContext();
        List<TransformHealthIssue> issues = new ArrayList<>();
        HealthStatus maxStatus = HealthStatus.GREEN;

        if (AuthorizationState.isNullOrGreen(authState) == false) {
            maxStatus = authState.getStatus();
            issues.add(IssueType.PRIVILEGES_CHECK_FAILED.newIssue(authState.getLastAuthError(), 1, null));
        }

        if (TransformTaskState.FAILED.equals(transformTask.getState().getTaskState())) {
            maxStatus = HealthStatus.RED;
            issues.add(
                IssueType.TRANSFORM_TASK_FAILED.newIssue(transformTask.getState().getReason(), 1, transformContext.getStateFailureTime())
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
                IssueType.TRANSFORM_INDEXER_FAILED.newIssue(
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
                IssueType.TRANSFORM_INTERNAL_STATE_UPDATE_FAILED.newIssue(
                    transformContext.getLastStatePersistenceFailure().getMessage(),
                    transformContext.getStatePersistenceFailureCount(),
                    transformContext.getLastStatePersistenceFailureStartTime()
                )
            );
        }

        if (transformContext.getStartUpFailureCount() != 0) {
            if (HealthStatus.RED.equals(maxStatus) == false) {
                maxStatus = HealthStatus.YELLOW;
            }

            var lastFailure = transformContext.getStartUpFailure();
            var lastFailureMessage = lastFailure instanceof ElasticsearchException elasticsearchException
                ? elasticsearchException.getDetailedMessage()
                : lastFailure.getMessage();
            issues.add(
                IssueType.TRANSFORM_STARTUP_FAILED.newIssue(
                    lastFailureMessage,
                    transformContext.getStartUpFailureCount(),
                    transformContext.getStartUpFailureTime()
                )
            );
        }

        return new TransformHealth(maxStatus, issues);
    }

    private TransformHealthChecker() {}
}
