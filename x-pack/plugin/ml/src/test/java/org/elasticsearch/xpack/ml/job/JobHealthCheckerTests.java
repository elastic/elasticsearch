/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.AnomalyDetectionHealth;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.CategorizationStatus;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats.MemoryStatus;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class JobHealthCheckerTests extends ESTestCase {

    public void testClosedJobIsGreen() {
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.CLOSED, null, null, null, null);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }

    public void testOpenedJobWithNoIssuesIsGreen() {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.OK).build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }

    public void testFailedJobIsRed() {
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.FAILED, null, null, "task failed due to exception", null);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(JobHealthChecker.IssueType.JOB_TASK_FAILED.type));
        assertThat(health.getIssues().get(0).getDetails(), is("task failed due to exception"));
    }

    public void testOpeningJobWithoutNodeIsRed() {
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENING, null, "not enough nodes with ML enabled", null, null);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(JobHealthChecker.IssueType.ASSIGNMENT_FAILED.type));
    }

    public void testModelMemorySoftLimitIsYellow() {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.SOFT_LIMIT).build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(JobHealthChecker.IssueType.MODEL_MEMORY_SOFT_LIMIT.type));
    }

    public void testModelMemoryHardLimitIsRed() {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.HARD_LIMIT).build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(JobHealthChecker.IssueType.MODEL_MEMORY_HARD_LIMIT.type));
    }

    public void testCategorizationWarnIsYellow() {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.OK)
            .setCategorizationStatus(CategorizationStatus.WARN)
            .build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.YELLOW));
        assertThat(health.getIssues(), hasSize(1));
        assertThat(health.getIssues().get(0).getType(), is(JobHealthChecker.IssueType.CATEGORIZATION_WARNING.type));
    }

    public void testHardLimitTakesPrecedenceOverSoftLimit() {
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.HARD_LIMIT).build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
    }

    public void testMultipleIssuesWorstStatusWins() {
        // Both HARD_LIMIT (RED) and CATEGORIZATION_WARNING (YELLOW) — overall must be RED
        ModelSizeStats modelSizeStats = new ModelSizeStats.Builder("job").setMemoryStatus(MemoryStatus.HARD_LIMIT)
            .setCategorizationStatus(CategorizationStatus.WARN)
            .build();
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, modelSizeStats);
        assertThat(health.getStatus(), is(HealthStatus.RED));
        assertThat(health.getIssues(), notNullValue());
        assertThat(health.getIssues().size(), is(2));
    }

    public void testNullModelSizeStatsDoesNotThrow() {
        AnomalyDetectionHealth health = JobHealthChecker.checkJob(JobState.OPENED, null, null, null, null);
        assertThat(health.getStatus(), is(HealthStatus.GREEN));
        assertThat(health.getIssues(), nullValue());
    }
}
