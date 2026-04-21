/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rerun;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.gradle.internal.test.rerun.model.FailedTestsReport;
import org.elasticsearch.gradle.internal.test.rerun.model.TestCase;
import org.elasticsearch.gradle.internal.test.rerun.model.WorkUnit;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Gradle plugin that implements smart test retries by filtering test execution based on
 * historical failure data from Develocity (Gradle Enterprise).
 * <p>
 * When a Buildkite job is retried, the pre-command hook fetches
 * failed test information from the Develocity API and creates a .failed-test-history.json file.
 * This plugin reads that file and configures test tasks to run only the tests that failed
 * in the previous attempt, significantly reducing retry time.
 * <p>
 * If no history file exists, all tests run normally. If a test task has no failures in the
 * history, it is skipped entirely.
 */
public abstract class InternalTestRerunPlugin implements Plugin<Project> {

    /**
     * File name for failed test history created by Buildkite pre-command hook.
     * This file is populated from Develocity API during job retries.
     */
    public static final String FAILED_TEST_HISTORY_FILENAME = ".failed-test-history.json";

    /**
     * Maximum file size for the failed test history JSON file (10MB).
     * This prevents potential DoS from malformed or malicious files.
     */
    private static final long MAX_JSON_FILE_SIZE = 10 * 1024 * 1024;

    @Override
    public void apply(Project project) {
        File settingsRoot = project.getLayout().getSettingsDirectory().getAsFile();

        Provider<RetryTestsBuildService> retryTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("retryTests", RetryTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(settingsRoot);
            });
        project.getTasks().withType(Test.class).configureEach(task -> configureTestTask(task, retryTestsProvider));
    }

    private static void configureTestTask(Test test, Provider<InternalTestRerunPlugin.RetryTestsBuildService> testsBuildServiceProvider) {
        FailedTestsReport failureReport = testsBuildServiceProvider.get().getFailureReport();
        if (failureReport == null) {
            // no historical test failures found
            test.getLogger().info("No failed test history found, running all tests");
            return;
        }

        // Branch order matters: a task in failedTestTasks is also in executedTestTasks, so
        // State 2 must be checked before State 3. Reversing these branches would classify a
        // Gradle-level failure (e.g. resource leak) as "confirmed passed" and skip it.
        WorkUnit workUnit = testsBuildServiceProvider.get().getWorkUnitForTask(test.getPath());
        if (workUnit != null) {
            // State 1: Task has recorded test failures — rerun only failed tests
            List<TestCase> tests = workUnit.tests();
            int totalTestCount = tests.stream().mapToInt(tc -> tc.children().size()).sum();
            test.getLogger().lifecycle("Smart retry: filtering to {} failed test classes ({} test methods)", tests.size(), totalTestCount);
            test.filter(testFilter -> {
                for (TestCase testClassCase : tests) {
                    if (testClassCase.name() == null) {
                        test.getLogger().warn("Skipping test class with null name in smart retry filter");
                        continue;
                    }
                    List<TestCase> children = testClassCase.children();
                    for (TestCase child : children) {
                        if (child.name() == null) {
                            test.getLogger().warn("Skipping test method with null name in class {}", testClassCase.name());
                            continue;
                        }
                        testFilter.includeTest(testClassCase.name(), child.name());
                    }
                }
            });
        } else if (testsBuildServiceProvider.get().wasTaskFailed(test.getPath())) {
            // State 2: Task failed at the Gradle level without test failures (e.g. resource leak
            // detected after tests passed) — run all tests to reproduce the failure
            test.getLogger()
                .lifecycle("Smart retry: running all tests for {} (task failed without test failures in previous run)", test.getPath());
        } else if (testsBuildServiceProvider.get().wasTaskExecuted(test.getPath())) {
            // State 3: Task was executed and passed — confirmed passed, skip it
            test.getLogger().lifecycle("Smart retry: skipping {} (confirmed passed in previous run)", test.getPath());
            test.onlyIf("Skipped by smart retry - confirmed passed in previous run", element -> false);
        } else {
            // State 4: Task was never executed (or executedTestTasks data unavailable) — run all tests
            test.getLogger().lifecycle("Smart retry: running all tests for {} (not executed in previous run)", test.getPath());
        }
    }

    public abstract static class RetryTestsBuildService implements BuildService<RetryTestsBuildService.Params> {

        private final FailedTestsReport failureReport;
        private final Map<String, WorkUnit> workUnitsByPath;
        private final Set<String> executedTestTasks;
        private final Set<String> failedTestTasks;

        interface Params extends BuildServiceParameters {
            RegularFileProperty getInfoPath();
        }

        public RetryTestsBuildService() {
            File failedTestsJsonFile = new File(getParameters().getInfoPath().getAsFile().get(), FAILED_TEST_HISTORY_FILENAME);
            if (failedTestsJsonFile.exists()) {
                // Validate file size to prevent DoS from malformed or malicious files
                long fileSize = failedTestsJsonFile.length();
                if (fileSize > MAX_JSON_FILE_SIZE) {
                    throw new RuntimeException(
                        String.format("Failed test history file too large: %d bytes (max: %d bytes)", fileSize, MAX_JSON_FILE_SIZE)
                    );
                }

                try {
                    // Configure ObjectMapper with stricter validation
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
                    this.failureReport = objectMapper.readValue(failedTestsJsonFile, FailedTestsReport.class);

                    // Build a HashMap for O(1) lookup instead of O(n) stream filtering
                    this.workUnitsByPath = this.failureReport.workUnits()
                        .stream()
                        .collect(Collectors.toMap(WorkUnit::name, Function.identity()));

                    // Build a HashSet from executedTestTasks for O(1) lookup.
                    // A null list means the data was unavailable (API call failed or old format),
                    // which triggers safe fallback (run all tests for unknown tasks).
                    List<String> executed = this.failureReport.executedTestTasks();
                    this.executedTestTasks = executed != null ? new HashSet<>(executed) : null;

                    // Build a HashSet from failedTestTasks for O(1) lookup.
                    // These are test tasks that failed at the Gradle level (e.g. resource leaks)
                    // but had no individual test failures.
                    List<String> failed = this.failureReport.failedTestTasks();
                    this.failedTestTasks = failed != null ? new HashSet<>(failed) : null;
                } catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to parse %s", FAILED_TEST_HISTORY_FILENAME), e);
                }
            } else {
                this.failureReport = null;
                this.workUnitsByPath = Collections.emptyMap();
                this.executedTestTasks = null;
                this.failedTestTasks = null;
            }
        }

        public FailedTestsReport getFailureReport() {
            return failureReport;
        }

        /**
         * Gets the work unit for a given test task path.
         * This uses a HashMap for O(1) lookup performance.
         *
         * @param taskPath the Gradle task path (e.g., ":server:test")
         * @return the WorkUnit for this task, or null if no failures recorded
         */
        public WorkUnit getWorkUnitForTask(String taskPath) {
            return workUnitsByPath.get(taskPath);
        }

        /**
         * Checks whether a test task was actually executed in the previous build run.
         * <p>
         * Returns {@code true} if the task was confirmed executed. Returns {@code false}
         * if the task was not executed OR if executed task data is unavailable (null),
         * which triggers the safe fallback of running all tests.
         *
         * @param taskPath the Gradle task path (e.g., ":server:test")
         * @return true if the task was confirmed executed in the previous run
         */
        public boolean wasTaskExecuted(String taskPath) {
            return executedTestTasks != null && executedTestTasks.contains(taskPath);
        }

        /**
         * Checks whether a test task failed at the Gradle level in the previous build run,
         * without having individual test failures. This covers cases like resource leak
         * detection, where all tests pass but the task fails during cluster shutdown.
         * <p>
         * Returns {@code false} if failed task data is unavailable (null), which triggers
         * the safe fallback of running all tests for that task via the
         * {@link #wasTaskExecuted} check.
         *
         * @param taskPath the Gradle task path (e.g., ":server:test")
         * @return true if the task failed without test failures in the previous run
         */
        public boolean wasTaskFailed(String taskPath) {
            return failedTestTasks != null && failedTestTasks.contains(taskPath);
        }
    }
}
