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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Gradle plugin that implements smart test retries by filtering test execution based on
 * historical failure data from Develocity (Gradle Enterprise).
 * <p>
 * When a Buildkite job is retried with SMART_RETRIES=true, the pre-command hook fetches
 * failed test information from the Develocity API and creates a .failed-test-history.json file.
 * This plugin reads that file and configures test tasks to run only the tests that failed
 * in the previous attempt, significantly reducing retry time.
 * <p>
 * If no history file exists, all tests run normally. If a test task has no failures in the
 * history, it is skipped entirely.
 */
public class InternalTestRerunPlugin implements Plugin<Project> {

    /**
     * File name for failed test history created by Buildkite pre-command hook.
     * This file is populated from Develocity API during job retries when SMART_RETRIES=true.
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

        WorkUnit workUnit = testsBuildServiceProvider.get().getWorkUnitForTask(test.getPath());
        if (workUnit != null) {
            List<TestCase> tests = workUnit.getTests();
            int totalTestCount = tests.stream().mapToInt(tc -> tc.getChildren().size()).sum();
            test.getLogger().lifecycle("Smart retry: filtering to {} failed test classes ({} test methods)", tests.size(), totalTestCount);

            test.filter(testFilter -> {
                for (TestCase testClassCase : tests) {
                    List<TestCase> children = testClassCase.getChildren();
                    for (TestCase child : children) {
                        testFilter.includeTest(testClassCase.getName(), child.getName());
                    }
                }
            });
        } else {
            test.getLogger().lifecycle("Smart retry: skipping {} (no failures in previous run)", test.getPath());
            test.onlyIf("Skipped by smart retry - no failures in previous run", element -> false);
        }
    }

    public abstract static class RetryTestsBuildService implements BuildService<RetryTestsBuildService.Params> {

        private final FailedTestsReport failureReport;
        private final Map<String, WorkUnit> workUnitsByPath;

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
                    this.workUnitsByPath = this.failureReport.getWorkUnits()
                        .stream()
                        .collect(Collectors.toMap(WorkUnit::getName, Function.identity()));
                } catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to parse %s", FAILED_TEST_HISTORY_FILENAME), e);
                }
            } else {
                this.failureReport = null;
                this.workUnitsByPath = Collections.emptyMap();
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
    }
}
