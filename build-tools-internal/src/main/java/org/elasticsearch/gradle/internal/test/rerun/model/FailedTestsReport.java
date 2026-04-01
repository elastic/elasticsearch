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
 * `api/tests/build/<buildId>?testOutcomes=failed`
 * <p>
 * The {@code executedTestTasks} field is populated from a separate API call to
 * {@code api/builds/<buildId>/gradle-test-performance} and contains the task paths
 * of all test tasks that actually executed in the previous build. This enables
 * three-state logic in the retry plugin:
 * <ul>
 *   <li>Task in workUnits: rerun only failed tests</li>
 *   <li>Task in executedTestTasks but not workUnits: skip (confirmed passed)</li>
 *   <li>Task not in executedTestTasks: run all tests (never executed)</li>
 *   <li>executedTestTasks is null: fallback to run all tests for unknown tasks</li>
 * </ul>
 * */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailedTestsReport(List<WorkUnit> workUnits, String testseed, List<String> executedTestTasks) {

    public FailedTestsReport(List<WorkUnit> workUnits, String testseed, List<String> executedTestTasks) {
        this.workUnits = workUnits != null ? workUnits : java.util.Collections.emptyList();
        this.testseed = testseed;
        this.executedTestTasks = executedTestTasks;
    }
}
