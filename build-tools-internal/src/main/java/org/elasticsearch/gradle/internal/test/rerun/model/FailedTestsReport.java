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
import java.util.Map;

/**
 * Shape of {@code .failed-test-history.json} produced by the smart-retry
 * pre-command script and consumed by {@code InternalTestRerunPlugin}.
 * <p>
 * {@code successfulTasks} lists Gradle task paths (e.g. {@code :server:test})
 * that completed successfully across all previous runs. The plugin skips any
 * test task in this list entirely.
 * <p>
 * {@code successfulTests} maps task paths to lists of individual test methods
 * (formatted as {@code "className#methodName"}) that passed within tasks that
 * were not fully successful. The plugin excludes these specific tests from
 * re-execution while running everything else in the task.
 *
 * @param successfulTasks task paths confirmed successful across previous runs; never {@code null} (defaulted to empty)
 * @param successfulTests per-task lists of passing test methods; never {@code null} (defaulted to empty)
 * @param testseed        the randomised test seed from the original build
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailedTestsReport(List<String> successfulTasks, Map<String, List<String>> successfulTests, String testseed) {

    public FailedTestsReport(List<String> successfulTasks, Map<String, List<String>> successfulTests, String testseed) {
        this.successfulTasks = successfulTasks != null ? successfulTasks : java.util.Collections.emptyList();
        this.successfulTests = successfulTests != null ? successfulTests : java.util.Collections.emptyMap();
        this.testseed = testseed;
    }
}
