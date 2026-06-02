/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.gradle.tooling.events.ProgressEvent;
import org.gradle.tooling.events.ProgressListener;
import org.gradle.tooling.events.task.TaskFailureResult;
import org.gradle.tooling.events.task.TaskFinishEvent;
import org.gradle.tooling.events.task.TaskOperationDescriptor;
import org.gradle.tooling.events.task.TaskSkippedResult;
import org.gradle.tooling.events.task.TaskStartEvent;
import org.gradle.tooling.events.task.TaskSuccessResult;
import org.gradle.tooling.events.test.JvmTestOperationDescriptor;
import org.gradle.tooling.events.test.TestFailureResult;
import org.gradle.tooling.events.test.TestFinishEvent;
import org.gradle.tooling.events.test.TestSkippedResult;
import org.gradle.tooling.events.test.TestSuccessResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Listens for task and test progress events from the Gradle Tooling API and records
 * the outcome of every task and test in execution order. Supports cancellation-aware
 * status tracking: tasks that fail during cancellation are marked INTERRUPTED, and
 * tasks that never started are marked NOT_RUN.
 */
public class TaskTracker implements ProgressListener {

    private final Map<String, TaskRecord> tasksByPath = new LinkedHashMap<>();
    private final Queue<TestRecord> testResults = new ConcurrentLinkedQueue<>();
    private final BuildCanceller canceller;

    public TaskTracker(BuildCanceller canceller) {
        this.canceller = canceller;
    }

    @Override
    public void statusChanged(ProgressEvent event) {
        if (event instanceof TaskStartEvent startEvent) {
            TaskOperationDescriptor descriptor = (TaskOperationDescriptor) startEvent.getDescriptor();
            String path = descriptor.getTaskPath();
            synchronized (tasksByPath) {
                tasksByPath.put(path, new TaskRecord(path));
            }
        } else if (event instanceof TaskFinishEvent finishEvent) {
            TaskOperationDescriptor descriptor = (TaskOperationDescriptor) finishEvent.getDescriptor();
            String path = descriptor.getTaskPath();
            synchronized (tasksByPath) {
                TaskRecord record = tasksByPath.get(path);
                if (record != null) {
                    record.finish(finishEvent, canceller.isCancelled());
                }
            }
        } else if (event instanceof TestFinishEvent testFinish) {
            if (testFinish.getDescriptor() instanceof JvmTestOperationDescriptor jvmDesc) {
                // Only record atomic (method-level) test results, not suites
                if (jvmDesc.getClassName() != null && jvmDesc.getMethodName() != null) {
                    String taskPath = findOwningTaskPath(jvmDesc);
                    String result;
                    if (testFinish.getResult() instanceof TestSuccessResult) {
                        result = "SUCCESS";
                    } else if (testFinish.getResult() instanceof TestFailureResult) {
                        result = "FAILURE";
                    } else if (testFinish.getResult() instanceof TestSkippedResult) {
                        result = "SKIPPED";
                    } else {
                        result = "SUCCESS";
                    }
                    testResults.add(new TestRecord(taskPath, jvmDesc.getClassName(), jvmDesc.getMethodName(), result));
                }
            }
        }
    }

    /**
     * Walks up the descriptor parent chain to find the owning task path. Test events are
     * nested under their task's operation descriptor in the Tooling API event hierarchy.
     */
    private String findOwningTaskPath(JvmTestOperationDescriptor descriptor) {
        var parent = descriptor.getParent();
        while (parent != null) {
            if (parent instanceof TaskOperationDescriptor taskDesc) {
                return taskDesc.getTaskPath();
            }
            parent = parent.getParent();
        }
        return "<unknown>";
    }

    /**
     * Generates the status report data matching the format written by the Gradle-internal
     * TaskStatusTrackerPlugin.
     */
    public StatusReport buildReport() {
        List<TaskRecord> tasks;
        synchronized (tasksByPath) {
            tasks = new ArrayList<>(tasksByPath.values());
        }

        // Collect all paths and ensure unstarted tasks are marked NOT_RUN
        Set<String> allPaths = new TreeSet<>();
        for (TaskRecord task : tasks) {
            allPaths.add(task.taskPath);
        }

        List<StatusReport.TaskEntry> taskEntries = allPaths.stream().map(path -> {
            TaskRecord record;
            synchronized (tasksByPath) {
                record = tasksByPath.get(path);
            }
            String outcome = record != null ? record.status.name() : TaskStatus.NOT_RUN.name();
            return new StatusReport.TaskEntry(path, outcome);
        }).toList();

        List<StatusReport.TestEntry> testEntries = testResults.stream()
            .sorted(Comparator.comparing(TestRecord::taskPath).thenComparing(TestRecord::className).thenComparing(TestRecord::methodName))
            .map(r -> new StatusReport.TestEntry(r.taskPath(), r.className(), r.methodName(), r.result()))
            .toList();

        return new StatusReport(
            taskEntries,
            testEntries,
            canceller.isCancelled(),
            GcpPreemptionWatchdog.preemptedAt() != null ? GcpPreemptionWatchdog.preemptedAt().toString() : null
        );
    }

    /**
     * Whether any task genuinely failed or any test reported a failure before
     * preemption cancelled the remaining work. INTERRUPTED tasks are not counted.
     */
    public boolean hadFailuresBeforePreemption() {
        synchronized (tasksByPath) {
            for (TaskRecord record : tasksByPath.values()) {
                if (record.status == TaskStatus.FAILED) {
                    return true;
                }
            }
        }
        for (TestRecord entry : testResults) {
            if ("FAILURE".equals(entry.result)) {
                return true;
            }
        }
        return false;
    }

    enum TaskStatus {
        STARTED,
        SUCCESS,
        UP_TO_DATE,
        FROM_CACHE,
        FAILED,
        SKIPPED,
        INTERRUPTED,
        NOT_RUN
    }

    static class TaskRecord {
        final String taskPath;
        TaskStatus status;

        TaskRecord(String taskPath) {
            this.taskPath = taskPath;
            this.status = TaskStatus.STARTED;
        }

        void finish(TaskFinishEvent event, boolean cancelled) {
            var result = event.getResult();
            if (result instanceof TaskSuccessResult successResult) {
                if (successResult.isFromCache()) {
                    this.status = TaskStatus.FROM_CACHE;
                } else if (successResult.isUpToDate()) {
                    this.status = TaskStatus.UP_TO_DATE;
                } else {
                    this.status = TaskStatus.SUCCESS;
                }
            } else if (result instanceof TaskFailureResult) {
                this.status = cancelled ? TaskStatus.INTERRUPTED : TaskStatus.FAILED;
            } else if (result instanceof TaskSkippedResult) {
                this.status = TaskStatus.SKIPPED;
            }
        }
    }

    record TestRecord(String taskPath, String className, String methodName, String result) {}
}
