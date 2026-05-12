/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * This reflects the model provided by develocity api call
 * {@code api/tests/build/<buildId>?testOutcomes=failed}.
 * <p>
 * The {@code executedTestTasks} field is populated from a separate API call to
 * {@code api/builds/<buildId>/gradle-test-performance} and contains the task paths
 * of all test tasks that actually executed in the previous build.
 * <p>
 * The {@code failedTestTasks} field is populated from
 * {@code api/builds/<buildId>/gradle-failures} (specifically {@code buildFailures[].taskPath})
 * and contains the task paths of test tasks that failed at the Gradle level (e.g.
 * resource leaks detected after tests passed). This enables four-state logic in the
 * retry plugin:
 * <ul>
 *   <li>Task in workUnits: rerun only failed tests</li>
 *   <li>Task in failedTestTasks but not workUnits: run all tests (non-test failure)</li>
 *   <li>Task in executedTestTasks but not workUnits or failedTestTasks: skip (confirmed passed)</li>
 *   <li>Task not in executedTestTasks: run all tests (never executed)</li>
 *   <li>executedTestTasks is null: fallback to run all tests for unknown tasks</li>
 * </ul>
 * <p>
 * <b>Null semantics:</b> {@code workUnits} is normalised to an empty list when the
 * API returns {@code null} (meaning "no failures"), so consumers can iterate it
 * unconditionally. {@code executedTestTasks} and {@code failedTestTasks} are
 * intentionally left nullable: {@code null} means the API data was unavailable
 * (fall back to running all tests), while an empty list means the API was
 * reachable but returned no entries.
 *
 * @param workUnits          list of failed test work-units; never {@code null} (defaulted to empty)
 * @param testseed           the randomised test seed from the original build
 * @param executedTestTasks  task paths of executed test tasks, or {@code null} if the data was unavailable
 * @param failedTestTasks    task paths of test tasks that failed at the Gradle level, or {@code null} if unavailable
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailedTestsReport(List<WorkUnit> workUnits, String testseed, List<String> executedTestTasks, List<String> failedTestTasks) {

    public FailedTestsReport(List<WorkUnit> workUnits, String testseed, List<String> executedTestTasks, List<String> failedTestTasks) {
        this.workUnits = workUnits != null ? workUnits : java.util.Collections.emptyList();
        this.testseed = testseed;
        this.executedTestTasks = executedTestTasks;
        this.failedTestTasks = failedTestTasks;
    }
}
