/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.ci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.internal.GradleInternal;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.build.event.BuildEventsListenerRegistry;
import org.gradle.initialization.BuildCancellationToken;
import org.gradle.tooling.events.FinishEvent;
import org.gradle.tooling.events.OperationCompletionListener;
import org.gradle.tooling.events.task.TaskFailureResult;
import org.gradle.tooling.events.task.TaskFinishEvent;
import org.gradle.tooling.events.task.TaskSkippedResult;
import org.gradle.tooling.events.task.TaskSuccessResult;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

/**
 * Tracks every task in the Gradle execution graph and writes a {@code .task-status.json} file
 * to the settings root directory at the end of the build, regardless of whether the build
 * succeeded, failed, or was cancelled.
 *
 * <p>Compatible with configuration cache and Gradle 10+. Does not use the deprecated
 * {@code beforeTask}/{@code afterTask} hooks on {@link org.gradle.api.execution.TaskExecutionGraph}.
 * Task events are received via {@link BuildEventsListenerRegistry#onTaskCompletion}, which is the
 * configuration-cache-safe replacement.
 *
 * <p>Must be applied to the root project only.
 */
public abstract class TaskStatusTrackerPlugin implements Plugin<Project> {

    @Inject
    public abstract BuildEventsListenerRegistry getListenerRegistry();

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(TaskStatusTrackerPlugin.class.getName() + " can only be applied to the root project.");
        }

        Provider<TaskStatusService> trackerProvider = project.getGradle()
            .getSharedServices()
            .registerIfAbsent("taskStatusTracker", TaskStatusService.class, spec -> {
                spec.getParameters().getOutputFile().set(project.getLayout().getBuildDirectory().file("task-status.json"));
            });

        // Config-cache-safe: Gradle re-subscribes the service to task events each build.
        getListenerRegistry().onTaskCompletion(trackerProvider);

        // Capture the full set of planned task paths once the graph is frozen.
        // Paths (strings) are safe to capture; Task objects must not be stored in a BuildService.
        project.getGradle().getTaskGraph().whenReady(graph -> {
            List<String> paths = graph.getAllTasks().stream().map(t -> t.getPath()).toList();
            trackerProvider.get().setPlannedTasks(paths);
        });

        // Best-effort explicit-cancellation detection using the same internal API as PreemptionBuildCanceller.
        // On config-cache hits apply() is not re-invoked, so this callback is absent; the close() fallback
        // still marks unstarted tasks as INTERRUPTED via the cancelled flag propagated by onFinish events.
        try {
            BuildCancellationToken token = ((GradleInternal) project.getGradle()).getServices().get(BuildCancellationToken.class);
            token.addCallback(() -> trackerProvider.get().markCancelled());
        } catch (Exception e) {
            // graceful degradation: cancelled tasks appear as NOT_RUN
        }

        // Add a TestListener to every Test task across all projects. Results accumulate in the
        // service and are flushed by writeReport(), including on GCP preemption.
        project.allprojects(p -> p.getTasks().withType(Test.class).configureEach(test -> {
            test.usesService(trackerProvider);
            test.addTestListener(new TestResultListener(test.getPath(), trackerProvider));
        }));
    }

    private static class TestResultListener implements TestListener {
        private final String taskPath;
        private final Provider<TaskStatusService> serviceProvider;

        TestResultListener(String taskPath, Provider<TaskStatusService> serviceProvider) {
            this.taskPath = taskPath;
            this.serviceProvider = serviceProvider;
        }

        @Override
        public void beforeSuite(TestDescriptor suite) {}

        @Override
        public void afterSuite(TestDescriptor suite, TestResult result) {}

        @Override
        public void beforeTest(TestDescriptor testDescriptor) {}

        @Override
        public void afterTest(TestDescriptor testDescriptor, TestResult result) {
            // isComposite() is true for class-level and root suite descriptors; skip those.
            if (testDescriptor.isComposite()) {
                return;
            }
            String className = testDescriptor.getClassName() != null ? testDescriptor.getClassName() : "<unknown>";
            serviceProvider.get().recordTestResult(taskPath, className, testDescriptor.getName(), result.getResultType().name());
        }
    }

    public enum TaskOutcome {
        SUCCESS,
        UP_TO_DATE,
        FROM_CACHE,
        FAILED,
        SKIPPED,
        /** Task was running when the build was explicitly cancelled (Ctrl+C or preemption). */
        INTERRUPTED,
        /** Task was planned but never dispatched before the build ended. */
        NOT_RUN
    }

    public abstract static class TaskStatusService
        implements
            BuildService<TaskStatusService.Params>,
            OperationCompletionListener,
            AutoCloseable {

        private static final Logger LOGGER = Logging.getLogger(TaskStatusService.class);

        private final Map<String, TaskOutcome> outcomes = new ConcurrentHashMap<>();
        private final Set<String> planned = ConcurrentHashMap.newKeySet();
        private final Queue<TaskStatusReport.TestEntry> testEntries = new ConcurrentLinkedQueue<>();
        private volatile boolean cancelled = false;

        interface Params extends BuildServiceParameters {
            RegularFileProperty getOutputFile();
        }

        public TaskStatusService() {
            // Register here rather than in apply() so the hook is always present regardless of
            // whether apply() was skipped by a config-cache hit. Fires on the watchdog thread,
            // which PreemptionBuildCanceller never interrupts, so the write is guaranteed to run
            // even when execution worker threads are interrupted during preemption.
            GcpPreemptionWatchdog.onPreempted(() -> {
                cancelled = true;
                writeReport();
            });
        }

        public void setPlannedTasks(Collection<String> paths) {
            planned.addAll(paths);
        }

        public void markCancelled() {
            cancelled = true;
        }

        public void recordTestResult(String taskPath, String className, String methodName, String result) {
            testEntries.add(new TaskStatusReport.TestEntry(taskPath, className, methodName, result));
        }

        @Override
        public void onFinish(FinishEvent event) {
            if (event instanceof TaskFinishEvent taskEvent) {
                String path = taskEvent.getDescriptor().getTaskPath();
                var result = taskEvent.getResult();
                TaskOutcome outcome;
                if (result instanceof TaskSuccessResult s) {
                    outcome = s.isFromCache() ? TaskOutcome.FROM_CACHE : s.isUpToDate() ? TaskOutcome.UP_TO_DATE : TaskOutcome.SUCCESS;
                } else if (result instanceof TaskFailureResult) {
                    // cancelled flag is set only by explicit cancellation (Ctrl+C / BuildCancellationToken).
                    // A task that gets a failure result while the build is being cancelled was interrupted.
                    outcome = cancelled ? TaskOutcome.INTERRUPTED : TaskOutcome.FAILED;
                } else if (result instanceof TaskSkippedResult) {
                    outcome = TaskOutcome.SKIPPED;
                } else {
                    outcome = TaskOutcome.SUCCESS;
                }
                outcomes.put(path, outcome);
            }
        }

        @Override
        public void close() {
            if (GcpPreemptionWatchdog.isPreempted()) {
                cancelled = true;
            }
            // Tasks with no onFinish event were never dispatched — always NOT_RUN regardless of
            // cancellation. INTERRUPTED is assigned in onFinish() for tasks that were actively
            // executing when the build was cancelled (they receive a TaskFailureResult there).
            for (String path : planned) {
                outcomes.putIfAbsent(path, TaskOutcome.NOT_RUN);
            }
            writeReport();
        }

        /**
         * Writes the current task status snapshot to disk. Called from both {@link #close()} (normal
         * path) and the {@link GcpPreemptionWatchdog#onPreempted} hook (preemption path). Synchronized
         * so concurrent calls from those two paths produce a complete file rather than interleaved writes.
         */
        private synchronized void writeReport() {
            // Tasks not yet in outcomes haven't started; NOT_RUN is correct at any point in time.
            TaskOutcome fallback = TaskOutcome.NOT_RUN;

            Set<String> allPaths = new TreeSet<>(planned);
            allPaths.addAll(outcomes.keySet());

            List<TaskStatusReport.TaskEntry> taskEntries = allPaths.stream()
                .map(path -> new TaskStatusReport.TaskEntry(path, outcomes.getOrDefault(path, fallback).name()))
                .toList();

            List<TaskStatusReport.TestEntry> tests = testEntries.stream()
                .sorted(
                    Comparator.comparing(TaskStatusReport.TestEntry::taskPath)
                        .thenComparing(TaskStatusReport.TestEntry::className)
                        .thenComparing(TaskStatusReport.TestEntry::methodName)
                )
                .toList();

            File outputFile = getParameters().getOutputFile().getAsFile().get();
            outputFile.getParentFile().mkdirs();
            try {
                new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
                    .writeValue(outputFile, new TaskStatusReport(taskEntries, tests, cancelled));
            } catch (IOException e) {
                LOGGER.warn("Failed to write task status report to {}", outputFile, e);
            }
        }

        /** Returns the live outcome map. Meaningful only after the build has finished. */
        public Map<String, TaskOutcome> getOutcomes() {
            return Collections.unmodifiableMap(outcomes);
        }
    }
}
