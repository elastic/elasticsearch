/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealth;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealthIssue;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizationStatus;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats.MemoryStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Computes the {@link AnomalyDetectionHealth} for a single anomaly detection job
 * from the individual stats fields already available during response assembly.
 * <p>
 * All checks are performed in-memory; no additional I/O is required.
 * </p>
 */
public final class JobHealthChecker {

    /** Utility class — not instantiable. */
    private JobHealthChecker() {}

    /**
     * Issue types recognised by this checker, together with their wire type strings,
     * human-readable titles, and default severity.
     */
    enum IssueType {
        ASSIGNMENT_FAILED("assignment_failed", "Failed to assign job to a node", HealthStatus.RED),
        JOB_TASK_FAILED("job_task_failed", "Job task state is [failed]", HealthStatus.RED),
        MODEL_MEMORY_SOFT_LIMIT("model_memory_soft_limit", "Model memory soft limit reached", HealthStatus.YELLOW),
        MODEL_MEMORY_HARD_LIMIT("model_memory_hard_limit", "Model memory hard limit reached", HealthStatus.RED),
        CATEGORIZATION_WARNING("categorization_warning", "Categorization status is [warn]", HealthStatus.YELLOW);

        final String type;
        final String title;
        final HealthStatus severity;

        IssueType(String type, String title, HealthStatus severity) {
            this.type = type;
            this.title = title;
            this.severity = severity;
        }
    }

    /**
     * Evaluates the health of a job and returns an {@link AnomalyDetectionHealth} instance.
     * A closed job is always {@code GREEN}. For all other states each potential issue is
     * checked in turn; the worst severity across all detected issues determines the
     * overall status.
     *
     * @param state                 the current {@link JobState}
     * @param node                  the node the job is assigned to, or {@code null} if unassigned
     * @param assignmentExplanation the assignment explanation string from the persistent task, may be {@code null}
     * @param failureReason         the failure reason from {@code JobTaskState}, populated when state is FAILED, may be {@code null}
     * @param modelSizeStats        current model size stats, or {@code null} if not yet available
     * @return the computed health object; never {@code null}
     */
    public static AnomalyDetectionHealth checkJob(
        JobState state,
        @Nullable DiscoveryNode node,
        @Nullable String assignmentExplanation,
        @Nullable String failureReason,
        @Nullable ModelSizeStats modelSizeStats
    ) {
        if (state == JobState.CLOSED) {
            return AnomalyDetectionHealth.GREEN;
        }

        List<AnomalyDetectionHealthIssue> issues = new ArrayList<>();
        HealthStatus maxStatus = HealthStatus.GREEN;

        // ASSIGNMENT_FAILED: job is not assigned to a node
        if (node == null && state == JobState.OPENING) {
            issues.add(
                new AnomalyDetectionHealthIssue(
                    IssueType.ASSIGNMENT_FAILED.type,
                    IssueType.ASSIGNMENT_FAILED.title,
                    assignmentExplanation,
                    1,
                    null
                )
            );
            maxStatus = max(maxStatus, IssueType.ASSIGNMENT_FAILED.severity);
        }

        // JOB_TASK_FAILED: the job's persistent task is in failed state
        if (state == JobState.FAILED) {
            issues.add(
                new AnomalyDetectionHealthIssue(IssueType.JOB_TASK_FAILED.type, IssueType.JOB_TASK_FAILED.title, failureReason, 1, null)
            );
            maxStatus = max(maxStatus, IssueType.JOB_TASK_FAILED.severity);
        }

        // MODEL_MEMORY_HARD_LIMIT / MODEL_MEMORY_SOFT_LIMIT / CATEGORIZATION_WARNING
        if (modelSizeStats != null) {
            MemoryStatus memoryStatus = modelSizeStats.getMemoryStatus();
            if (memoryStatus == MemoryStatus.HARD_LIMIT) {
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.MODEL_MEMORY_HARD_LIMIT.type,
                        IssueType.MODEL_MEMORY_HARD_LIMIT.title,
                        hardLimitDetails(modelSizeStats),
                        1,
                        null
                    )
                );
                maxStatus = max(maxStatus, IssueType.MODEL_MEMORY_HARD_LIMIT.severity);
            } else if (memoryStatus == MemoryStatus.SOFT_LIMIT) {
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.MODEL_MEMORY_SOFT_LIMIT.type,
                        IssueType.MODEL_MEMORY_SOFT_LIMIT.title,
                        softLimitDetails(modelSizeStats),
                        1,
                        null
                    )
                );
                maxStatus = max(maxStatus, IssueType.MODEL_MEMORY_SOFT_LIMIT.severity);
            }

            if (modelSizeStats.getCategorizationStatus() == CategorizationStatus.WARN) {
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.CATEGORIZATION_WARNING.type,
                        IssueType.CATEGORIZATION_WARNING.title,
                        categorizationDetails(modelSizeStats),
                        1,
                        null
                    )
                );
                maxStatus = max(maxStatus, IssueType.CATEGORIZATION_WARNING.severity);
            }
        }

        if (issues.isEmpty()) {
            return AnomalyDetectionHealth.GREEN;
        }
        return new AnomalyDetectionHealth(maxStatus, issues);
    }

    private static String softLimitDetails(ModelSizeStats modelSizeStats) {
        Long memoryLimit = modelSizeStats.getModelBytesMemoryLimit();
        if (memoryLimit == null) {
            return "The model is using aggressive pruning to stay within the memory limit. Results accuracy may be reduced.";
        }
        return "Job requires approximately "
            + modelSizeStats.getModelBytes()
            + " bytes of memory. The limit is "
            + memoryLimit
            + " bytes. The model is using aggressive pruning to stay within the limit. Results accuracy may be reduced.";
    }

    private static String hardLimitDetails(ModelSizeStats modelSizeStats) {
        Long memoryLimit = modelSizeStats.getModelBytesMemoryLimit();
        Long bytesExceeded = modelSizeStats.getModelBytesExceeded();
        if (memoryLimit == null) {
            return "The model has exceeded its memory limit. Some model data may have been dropped.";
        }
        String base = "Job requires approximately "
            + modelSizeStats.getModelBytes()
            + " bytes of memory. The limit is "
            + memoryLimit
            + " bytes.";
        if (bytesExceeded != null && bytesExceeded > 0) {
            base += " An estimated " + bytesExceeded + " bytes of model memory have been dropped.";
        }
        return base;
    }

    private static String categorizationDetails(ModelSizeStats modelSizeStats) {
        return "Categorization status is ["
            + modelSizeStats.getCategorizationStatus().name().toLowerCase(Locale.ROOT)
            + "]. Total categories: "
            + modelSizeStats.getTotalCategoryCount()
            + ", frequent: "
            + modelSizeStats.getFrequentCategoryCount()
            + ", rare: "
            + modelSizeStats.getRareCategoryCount()
            + ", dead: "
            + modelSizeStats.getDeadCategoryCount()
            + ".";
    }

    private static HealthStatus max(HealthStatus a, HealthStatus b) {
        return a.value() >= b.value() ? a : b;
    }
}
