/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Response.DatafeedProblemStats;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealth;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealthIssue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes the {@link AnomalyDetectionHealth} for a single datafeed from the stats
 * fields already assembled during the {@code GET _ml/datafeeds/_stats} response.
 * <p>
 * All checks are performed in-memory; no additional I/O is required.
 * </p>
 */
public final class DatafeedHealthChecker {

    /**
     * Failure count above which a recurring error is escalated from YELLOW to RED.
     * Mirrors the same boundary used in TransformHealthChecker.
     */
    static final int RED_STATUS_FAILURE_COUNT_BOUNDARY = 5;

    /** Utility class — not instantiable. */
    private DatafeedHealthChecker() {}

    /**
     * Issue types recognised by this checker, together with their wire type strings,
     * human-readable titles, and default severity.
     */
    enum IssueType {
        ASSIGNMENT_FAILED("assignment_failed", "Failed to assign datafeed to a node", HealthStatus.RED),
        DATA_EXTRACTION_ERROR("data_extraction_error", "Data extraction error", HealthStatus.YELLOW),
        DATA_ANALYSIS_ERROR("data_analysis_error", "Data analysis error", HealthStatus.YELLOW),
        DATA_DELAY("data_delay", "Datafeed is experiencing delayed data ingestion", HealthStatus.YELLOW),
        EMPTY_DATA("empty_data", "Datafeed has not received data for some time", HealthStatus.YELLOW);

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
     * Evaluates the health of a datafeed and returns an {@link AnomalyDetectionHealth} instance.
     * A stopped datafeed is always {@code GREEN}.
     *
     * @param datafeedState         the current {@link DatafeedState}
     * @param node                  the node the datafeed is assigned to, or {@code null} if unassigned
     * @param assignmentExplanation the assignment explanation from the persistent task, may be {@code null}
     * @param problemStats          snapshot of failure and delayed-data counters from the datafeed task, may be {@code null}
     * @return the computed health object; never {@code null}
     */
    public static AnomalyDetectionHealth checkDatafeed(
        DatafeedState datafeedState,
        @Nullable DiscoveryNode node,
        @Nullable String assignmentExplanation,
        @Nullable DatafeedProblemStats problemStats
    ) {
        if (datafeedState == DatafeedState.STOPPED || datafeedState == DatafeedState.STOPPING) {
            return AnomalyDetectionHealth.GREEN;
        }

        List<AnomalyDetectionHealthIssue> issues = new ArrayList<>();
        HealthStatus maxStatus = HealthStatus.GREEN;

        // ASSIGNMENT_FAILED: datafeed is starting but has no node assigned
        if (node == null && datafeedState == DatafeedState.STARTING) {
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

        if (problemStats != null) {
            // DATA_EXTRACTION_ERROR: escalates to RED after RED_STATUS_FAILURE_COUNT_BOUNDARY failures
            int extractionCount = problemStats.getExtractionFailureCount();
            if (extractionCount > 0) {
                HealthStatus severity = extractionCount > RED_STATUS_FAILURE_COUNT_BOUNDARY ? HealthStatus.RED : HealthStatus.YELLOW;
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.DATA_EXTRACTION_ERROR.type,
                        IssueType.DATA_EXTRACTION_ERROR.title,
                        null,
                        extractionCount,
                        problemStats.getExtractionFailureFirstTime()
                    )
                );
                maxStatus = max(maxStatus, severity);
            }

            // DATA_ANALYSIS_ERROR: RED immediately if any failure was fatal (conflict closed the job),
            // or after RED_STATUS_FAILURE_COUNT_BOUNDARY cumulative failures
            int analysisCount = problemStats.getAnalysisFailureCount();
            if (analysisCount > 0) {
                HealthStatus severity = (problemStats.isAnalysisFailureFatal() || analysisCount > RED_STATUS_FAILURE_COUNT_BOUNDARY)
                    ? HealthStatus.RED
                    : HealthStatus.YELLOW;
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.DATA_ANALYSIS_ERROR.type,
                        IssueType.DATA_ANALYSIS_ERROR.title,
                        null,
                        analysisCount,
                        problemStats.getAnalysisFailureFirstTime()
                    )
                );
                maxStatus = max(maxStatus, severity);
            }

            // EMPTY_DATA: YELLOW only after 10 consecutive empty cycles
            int emptyCount = problemStats.getEmptyDataCount();
            if (emptyCount >= 10) {
                issues.add(new AnomalyDetectionHealthIssue(IssueType.EMPTY_DATA.type, IssueType.EMPTY_DATA.title, null, emptyCount, null));
                maxStatus = max(maxStatus, IssueType.EMPTY_DATA.severity);
            }

            // DATA_DELAY: delayed data detected by the DelayedDataDetector in the last triggered check
            int delayedCount = problemStats.getDelayedDataBucketCount();
            if (delayedCount > 0) {
                issues.add(
                    new AnomalyDetectionHealthIssue(
                        IssueType.DATA_DELAY.type,
                        IssueType.DATA_DELAY.title,
                        delayedDataDetails(problemStats),
                        delayedCount,
                        problemStats.getDelayedDataFirstOccurrence()
                    )
                );
                maxStatus = max(maxStatus, IssueType.DATA_DELAY.severity);
            }
        }

        if (issues.isEmpty()) {
            return AnomalyDetectionHealth.GREEN;
        }
        return new AnomalyDetectionHealth(maxStatus, issues);
    }

    private static String delayedDataDetails(DatafeedProblemStats problemStats) {
        long missingCount = problemStats.getLastDelayedDataMissingCount();
        long bucketEndMs = problemStats.getLastDelayedDataBucketEndMs();
        if (missingCount <= 0 || bucketEndMs <= 0) {
            return null;
        }
        return missingCount
            + " document"
            + (missingCount == 1 ? "" : "s")
            + " missing. Latest affected bucket ended at "
            + Instant.ofEpochMilli(bucketEndMs)
            + ". Consider increasing query_delay.";
    }

    private static HealthStatus max(HealthStatus a, HealthStatus b) {
        return a.value() >= b.value() ? a : b;
    }
}
