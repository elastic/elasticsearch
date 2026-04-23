/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Response.DatafeedProblemStats;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealth;

import java.time.Instant;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedHealthCheckerTests extends ESTestCase {

    public void testStoppedDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STOPPED, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }

    public void testStoppingDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STOPPING, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testStartedWithNoIssuesIsGreen() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 0);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testAssignmentFailedIsRed() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTING, null, "no ML nodes available", null
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.ASSIGNMENT_FAILED.type));
    }

    public void testExtractionFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY,
            Instant.now(),
            0, null, 0, false, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_EXTRACTION_ERROR.type));
    }

    public void testExtractionFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1,
            Instant.now(),
            0, null, 0, false, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues().get(0).getCount(), is(DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1));
    }

    public void testAnalysisFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY, Instant.now(), 0, false, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }

    public void testAnalysisFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1, Instant.now(), 0, false, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
    }

    public void testFatalAnalysisFailureIsRedEvenBelowBoundary() {
        // A single fatal analysis failure (conflict that closed the job) must be RED immediately
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 1, Instant.now(), 0, true, 0);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }

    public void testEmptyDataBelowThresholdIsGreen() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 9, false, 0);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testEmptyDataAtThresholdIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 10, false, 0);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.EMPTY_DATA.type));
    }

    public void testDelayedDataIsYellow() {
        // First occurrence: count = 1
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 1);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_DELAY.type));
        assertThat(health.getIssues().get(0).getCount(), is(1));
    }

    public void testDelayedDataCountIsPreserved() {
        // Multiple consecutive delayed buckets — count must be surfaced correctly
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 5);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getCount(), is(5));
    }

    public void testDelayedDataNeverExceedsYellow() {
        // Even a very large count must not produce RED
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 1000);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
    }

    public void testNullProblemStatsDoesNotThrow() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }
}
