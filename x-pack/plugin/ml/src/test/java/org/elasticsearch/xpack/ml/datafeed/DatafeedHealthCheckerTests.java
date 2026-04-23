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
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealth;

import java.time.Instant;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedHealthCheckerTests extends ESTestCase {

    public void testStoppedDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STOPPED, null, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }

    public void testStoppingDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STOPPING, null, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testStartedWithNoIssuesIsGreen() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testAssignmentFailedIsRed() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTING, null, "no ML nodes available", null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.ASSIGNMENT_FAILED.type));
    }

    public void testExtractionFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY,
            Instant.now(),
            0, null, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_EXTRACTION_ERROR.type));
    }

    public void testExtractionFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1,
            Instant.now(),
            0, null, 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues().get(0).getCount(), is(DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1));
    }

    public void testAnalysisFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY, Instant.now(), 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }

    public void testAnalysisFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1, Instant.now(), 0
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.RED));
    }

    public void testEmptyDataBelowThresholdIsGreen() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 9);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testEmptyDataAtThresholdIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 10);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, stats
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.EMPTY_DATA.type));
    }

    public void testDataDelayIsYellow() {
        // Search interval that ended 2 minutes ago
        long twoMinutesAgoMs = Instant.now().toEpochMilli() - 120_000L;
        SearchInterval laggedInterval = new SearchInterval(twoMinutesAgoMs - 60_000L, twoMinutesAgoMs);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, laggedInterval, null
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_DELAY.type));
    }

    public void testDataDelayNeverExceedsYellow() {
        // Even a very large lag must not produce RED
        long longAgoMs = Instant.now().toEpochMilli() - 3_600_000L; // 1 hour ago
        SearchInterval laggedInterval = new SearchInterval(longAgoMs - 60_000L, longAgoMs);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, laggedInterval, null
        );
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
    }

    public void testNullProblemStatsDoesNotThrow() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(
            DatafeedState.STARTED, null, null, null, null
        );
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }
}
