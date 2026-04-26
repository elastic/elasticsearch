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
import java.time.temporal.ChronoUnit;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DatafeedHealthCheckerTests extends ESTestCase {

    /** Convenience: build a stats object with no issues at all. */
    private static DatafeedProblemStats noIssues() {
        return new DatafeedProblemStats(0, null, 0, null, 0, false, 0, null, 0L, 0L);
    }

    public void testStoppedDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STOPPED, null, null, null);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }

    public void testStoppingDatafeedIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STOPPING, null, null, null);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testStartedWithNoIssuesIsGreen() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, noIssues());
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testAssignmentFailedIsRed() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTING, null, "no ML nodes available", null);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.ASSIGNMENT_FAILED.type));
    }

    public void testExtractionFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY,
            Instant.now(),
            0,
            null,
            0,
            false,
            0,
            null,
            0L,
            0L
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_EXTRACTION_ERROR.type));
    }

    public void testExtractionFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1,
            Instant.now(),
            0,
            null,
            0,
            false,
            0,
            null,
            0L,
            0L
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues().get(0).getCount(), is(DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1));
    }

    public void testAnalysisFailuresBelowBoundaryIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            0,
            null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY,
            Instant.now(),
            0,
            false,
            0,
            null,
            0L,
            0L
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }

    public void testAnalysisFailuresAboveBoundaryIsRed() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            0,
            null,
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY + 1,
            Instant.now(),
            0,
            false,
            0,
            null,
            0L,
            0L
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
    }

    public void testFatalAnalysisFailureIsRedEvenBelowBoundary() {
        // A single fatal analysis failure (conflict that closed the job) must be RED immediately
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 1, Instant.now(), 0, true, 0, null, 0L, 0L);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }

    public void testEmptyDataBelowThresholdIsGreen() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 9, false, 0, null, 0L, 0L);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testEmptyDataAtThresholdIsYellow() {
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 10, false, 0, null, 0L, 0L);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.EMPTY_DATA.type));
    }

    public void testDelayedDataIsYellow() {
        Instant firstOccurrence = Instant.now().minusSeconds(300);
        long bucketEndMs = Instant.now().toEpochMilli();
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 1, firstOccurrence, 42L, bucketEndMs);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(DatafeedHealthChecker.IssueType.DATA_DELAY.type));
        assertThat(health.getIssues().get(0).getCount(), is(1));
        assertThat(health.getIssues().get(0).getFirstOccurrence(), is(firstOccurrence.truncatedTo(ChronoUnit.MILLIS)));
    }

    public void testDelayedDataDetailsContainsMissingCountAndTimestamp() {
        long bucketEndMs = 1_700_000_000_000L; // fixed epoch-ms for deterministic assertion
        DatafeedProblemStats stats = new DatafeedProblemStats(0, null, 0, null, 0, false, 1, Instant.now(), 42L, bucketEndMs);
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        String details = health.getIssues().get(0).getDetails();
        assertThat(details, notNullValue());
        assertThat(details, containsString("42"));
        assertThat(details, containsString(Instant.ofEpochMilli(bucketEndMs).toString()));
        assertThat(details, containsString("query_delay"));
    }

    public void testDelayedDataCountIsPreserved() {
        DatafeedProblemStats stats = new DatafeedProblemStats(
            0,
            null,
            0,
            null,
            0,
            false,
            5,
            Instant.now(),
            100L,
            Instant.now().toEpochMilli()
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues().get(0).getCount(), is(5));
    }

    public void testDelayedDataNeverExceedsYellow() {
        // Even a very large count must not produce RED
        DatafeedProblemStats stats = new DatafeedProblemStats(
            0,
            null,
            0,
            null,
            0,
            false,
            1000,
            Instant.now(),
            999L,
            Instant.now().toEpochMilli()
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
    }

    public void testNullProblemStatsDoesNotThrow() {
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, null);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
    }

    public void testMultipleIssuesOverallStatusIsWorstSeverity() {
        // YELLOW extraction + RED fatal analysis → overall RED with both issues present
        DatafeedProblemStats stats = new DatafeedProblemStats(
            DatafeedHealthChecker.RED_STATUS_FAILURE_COUNT_BOUNDARY,
            Instant.now(),
            1,
            Instant.now(),
            0,
            true, // fatal analysis failure → RED immediately
            0,
            null,
            0L,
            0L
        );
        AnomalyDetectionHealth health = DatafeedHealthChecker.checkDatafeed(DatafeedState.STARTED, null, null, stats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(2));
        List<String> types = health.getIssues().stream().map(i -> i.getType()).collect(Collectors.toList());
        assertThat(types, hasItem(DatafeedHealthChecker.IssueType.DATA_EXTRACTION_ERROR.type));
        assertThat(types, hasItem(DatafeedHealthChecker.IssueType.DATA_ANALYSIS_ERROR.type));
    }
}
