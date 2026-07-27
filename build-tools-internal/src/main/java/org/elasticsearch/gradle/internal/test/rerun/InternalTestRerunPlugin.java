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
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.execution.TaskExecutionGraph;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Gradle plugin that implements smart test retries by skipping test tasks and
 * individual tests that succeeded in previous build attempts.
 * <p>
 * When a Buildkite job is retried, the pre-command hook produces a
 * {@code .failed-test-history.json} file listing tasks and individual tests that
 * completed successfully. This plugin reads that file and:
 * <ul>
 *   <li>Skips entire test tasks listed in {@code successfulTasks}</li>
 *   <li>Excludes individual passing tests listed in {@code successfulTests}
 *       from partially-failed tasks</li>
 *   <li>Runs everything else normally</li>
 * </ul>
 * If no history file exists, all tests run normally.
 */
public abstract class InternalTestRerunPlugin implements Plugin<Project> {

    public static final String FAILED_TEST_HISTORY_FILENAME = ".failed-test-history.json";

    // Guard against OOM: the entire file is read into memory and held for the whole build in the long-lived Gradle daemon.
    private static final long MAX_JSON_FILE_SIZE = 100 * 1024 * 1024;

    @Override
    public void apply(Project project) {
        File settingsRoot = project.getLayout().getSettingsDirectory().getAsFile();

        Provider<RetryTestsBuildService> retryTestsProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("retryTests", RetryTestsBuildService.class, spec -> {
                spec.getParameters().getInfoPath().set(settingsRoot);
            });
        project.getTasks().withType(Test.class).configureEach(task -> configureTestTask(task, retryTestsProvider));

        // A successful task must NOT be skipped if a task that is actually going to run (transitively) depends on it.
        // Some test tasks share mutable external state with their dependencies (e.g. rolling-upgrade tasks each upgrade
        // one node of a shared cluster in doFirst), so skipping a dependency leaves that state in the wrong shape and
        // fails the dependent task. The reverse-dependency information is only available once the whole task graph is
        // built, so we resolve it in whenReady and consult it from each task's onlyIf predicate at execution time.
        project.getGradle().getTaskGraph().whenReady(graph -> resolveForceRunTasks(graph, retryTestsProvider.get()));
    }

    /**
     * Walks the resolved task graph and marks every successful test task that is a (transitive) dependency of a task
     * that will actually run as "force run", so it is not skipped by smart retry.
     * <p>
     * This relies on the configuration phase running (the resolved set is held in the build service instance used
     * during that phase). That is always the case for smart retry: the {@code .failed-test-history.json} file is
     * not a tracked configuration-cache input, so a retry writing that file runs with a configuration-cache miss.
     */
    private static void resolveForceRunTasks(TaskExecutionGraph graph, RetryTestsBuildService service) {
        // whenReady is registered once per project applying this plugin, but the graph is build-wide, so resolve once.
        if (service.markForceRunResolved() == false) {
            return;
        }
        // getAllTasks() is the set of tasks that will actually run; getDependencies(task) may return tasks that exist
        // but are not scheduled, and calling getDependencies on such a task throws, so we only traverse scheduled tasks.
        Set<Task> scheduledTasks = new HashSet<>(graph.getAllTasks());
        Set<String> forceRun = new HashSet<>();
        Set<Task> visited = new HashSet<>();
        for (Task task : scheduledTasks) {
            boolean willBeSkipped = task instanceof Test && service.wasTaskSuccessful(task.getPath());
            if (willBeSkipped == false) {
                collectSuccessfulDependencies(graph, task, service, forceRun, visited, scheduledTasks);
            }
        }
        service.setForceRunTasks(forceRun);
    }

    private static void collectSuccessfulDependencies(
        TaskExecutionGraph graph,
        Task task,
        RetryTestsBuildService service,
        Set<String> forceRun,
        Set<Task> visited,
        Set<Task> scheduledTasks
    ) {
        for (Task dependency : graph.getDependencies(task)) {
            // A dependency may exist without being scheduled to run; it cannot cause a skipped task to run and
            // querying its own dependencies would throw, so ignore it.
            if (scheduledTasks.contains(dependency) == false || visited.add(dependency) == false) {
                continue;
            }
            if (dependency instanceof Test && service.wasTaskSuccessful(dependency.getPath())) {
                forceRun.add(dependency.getPath());
            }
            collectSuccessfulDependencies(graph, dependency, service, forceRun, visited, scheduledTasks);
        }
    }

    private static void configureTestTask(Test test, Provider<RetryTestsBuildService> testsBuildServiceProvider) {
        FailedTestsReport report = testsBuildServiceProvider.get().getReport();
        if (report == null) {
            test.getLogger().info("No failed test history found, running all tests");
            return;
        }

        if (test.getPath().endsWith("remote-cluster") || test.getPath().endsWith("mixed-cluster")) {
            test.getLogger().lifecycle("Smart retry: running all tests for {} (multi-cluster task, never skipped)", test.getPath());
            return;
        }

        if (testsBuildServiceProvider.get().wasTaskSuccessful(test.getPath())) {
            test.getLogger()
                .lifecycle("Smart retry: {} succeeded in previous run (will skip unless a running task depends on it)", test.getPath());
            // Skip unless a task that will actually run depends on this one (resolved from the task graph in whenReady).
            test.onlyIf(
                "Skipped by smart retry - succeeded in previous run and no running task depends on it",
                element -> testsBuildServiceProvider.get().isForceRun(element.getPath())
            );
            return;
        }

        List<String> suitesToExclude = testsBuildServiceProvider.get().getSuccessfulSuitesForTask(test.getPath());
        List<String> testsToExclude = testsBuildServiceProvider.get().getSuccessfulTestsForTask(test.getPath());
        if (suitesToExclude.isEmpty() == false || testsToExclude.isEmpty() == false) {
            test.getLogger()
                .lifecycle(
                    "Smart retry: excluding {} successful suites and {} successful tests from {} (rerunning failures)",
                    suitesToExclude.size(),
                    testsToExclude.size(),
                    test.getPath()
                );
            test.filter(filter -> {
                for (String className : suitesToExclude) {
                    filter.excludeTestsMatching(className + ".*");
                }
                for (String testRef : testsToExclude) {
                    int hashIdx = testRef.indexOf('#');
                    if (hashIdx < 0) {
                        test.getLogger().warn("Skipping malformed test reference in smart retry: {}", testRef);
                        continue;
                    }
                    String className = testRef.substring(0, hashIdx);
                    String methodName = testRef.substring(hashIdx + 1);
                    filter.excludeTestsMatching(className + "." + methodName);
                }
            });
        } else {
            test.getLogger().lifecycle("Smart retry: running all tests for {} (not confirmed successful in previous run)", test.getPath());
        }
    }

    public abstract static class RetryTestsBuildService implements BuildService<RetryTestsBuildService.Params> {

        private final FailedTestsReport report;
        private final Set<String> successfulTasks;
        private final Map<String, List<String>> successfulSuites;
        private final Map<String, List<String>> successfulTests;

        // Successful tasks that must still run because a running task depends on them. Populated once the task graph is
        // resolved (see resolveForceRunTasks) and read from onlyIf predicates at execution time.
        private final Set<String> forceRunTasks = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean forceRunResolved = new AtomicBoolean(false);

        interface Params extends BuildServiceParameters {
            RegularFileProperty getInfoPath();
        }

        public RetryTestsBuildService() {
            File failedTestsJsonFile = new File(getParameters().getInfoPath().getAsFile().get(), FAILED_TEST_HISTORY_FILENAME);
            if (failedTestsJsonFile.exists()) {
                long fileSize = failedTestsJsonFile.length();
                if (fileSize > MAX_JSON_FILE_SIZE) {
                    throw new RuntimeException(
                        String.format("Failed test history file too large: %d bytes (max: %d bytes)", fileSize, MAX_JSON_FILE_SIZE)
                    );
                }

                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true);
                    this.report = objectMapper.readValue(failedTestsJsonFile, FailedTestsReport.class);
                    this.successfulTasks = new HashSet<>(this.report.successfulTasks());
                    this.successfulSuites = this.report.successfulSuites();
                    this.successfulTests = this.report.successfulTests();
                } catch (IOException e) {
                    throw new RuntimeException(String.format("Failed to parse %s", FAILED_TEST_HISTORY_FILENAME), e);
                }
            } else {
                this.report = null;
                this.successfulTasks = Set.of();
                this.successfulSuites = Map.of();
                this.successfulTests = Map.of();
            }
        }

        public FailedTestsReport getReport() {
            return report;
        }

        public boolean wasTaskSuccessful(String taskPath) {
            return successfulTasks.contains(taskPath);
        }

        public List<String> getSuccessfulSuitesForTask(String taskPath) {
            return successfulSuites.getOrDefault(taskPath, Collections.emptyList());
        }

        public List<String> getSuccessfulTestsForTask(String taskPath) {
            return successfulTests.getOrDefault(taskPath, Collections.emptyList());
        }

        /** Returns {@code true} exactly once, so the (build-wide) force-run resolution runs only for the first caller. */
        public boolean markForceRunResolved() {
            return forceRunResolved.compareAndSet(false, true);
        }

        public void setForceRunTasks(Set<String> taskPaths) {
            forceRunTasks.addAll(taskPaths);
        }

        /** Whether a successful task must still run because a running task (transitively) depends on it. */
        public boolean isForceRun(String taskPath) {
            return forceRunTasks.contains(taskPath);
        }
    }
}
