/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Batching support for {@link PrioritizedEsThreadPoolExecutor}
 * Tasks that share the same batching key are batched (see {@link BatchedTask#batchingKey})
 */
public abstract class TaskBatcher {

    private final Logger logger;
    private final PrioritizedEsThreadPoolExecutor threadExecutor;
    // package visible for tests
    final Map<Object, Set<BatchedTask>> tasksPerBatchingKey = new ConcurrentHashMap<>();

    public TaskBatcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
    }

    public void submitTasks(List<? extends BatchedTask> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchedTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey)
            : "tasks submitted in a batch should share the same batching key: " + tasks;
        // convert to an identity map to check for dups based on task identity

        tasksPerBatchingKey.compute(firstTask.batchingKey, (k, existingTasks) -> {
            assert assertNoDuplicateTasks(tasks, existingTasks);
            if (existingTasks == null) {
                return Collections.synchronizedSet(new LinkedHashSet<>(tasks));
            }
            existingTasks.addAll(tasks);
            return existingTasks;
        });

        if (timeout != null) {
            threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    private static boolean assertNoDuplicateTasks(List<? extends BatchedTask> tasks, Set<BatchedTask> existingTasks) {
        final Map<Object, BatchedTask> tasksIdentity = tasks.stream()
            .collect(
                Collectors.toMap(
                    BatchedTask::getTask,
                    Function.identity(),
                    (a, b) -> { throw new AssertionError("cannot add duplicate task: " + a); },
                    IdentityHashMap::new
                )
            );
        if (existingTasks == null) {
            return true;
        }
        for (BatchedTask existing : existingTasks) {
            // check that there won't be two tasks with the same identity for the same batching key
            BatchedTask duplicateTask = tasksIdentity.get(existing.getTask());
            assert duplicateTask == null
                : "task ["
                    + duplicateTask.describeTasks(Collections.singletonList(existing))
                    + "] with source ["
                    + duplicateTask.source
                    + "] is already queued";
        }
        return true;
    }

    private void onTimeoutInternal(List<? extends BatchedTask> tasks, TimeValue timeout) {
        final ArrayList<BatchedTask> toRemove = new ArrayList<>();
        for (BatchedTask task : tasks) {
            if (task.processed.getAndSet(true) == false) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                toRemove.add(task);
            }
        }
        if (toRemove.isEmpty() == false) {
            BatchedTask firstTask = toRemove.get(0);
            Object batchingKey = firstTask.batchingKey;
            assert tasks.stream().allMatch(t -> t.batchingKey == batchingKey)
                : "tasks submitted in a batch should share the same batching key: " + tasks;
            tasksPerBatchingKey.computeIfPresent(batchingKey, (key, existingTasks) -> {
                toRemove.forEach(existingTasks::remove);
                if (existingTasks.isEmpty()) {
                    return null;
                }
                return existingTasks;
            });
            onTimeout(toRemove, timeout);
        }
    }

    /**
     * Action to be implemented by the specific batching implementation.
     * All tasks have the same batching key.
     */
    protected abstract void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout);

    void runIfNotProcessed(BatchedTask updateTask) {
        // if this task is already processed, it shouldn't execute other tasks with same batching key that arrived later,
        // to give other tasks with different batching key a chance to execute.
        if (updateTask.processed.get() == false) {
            final List<BatchedTask> toExecute = new ArrayList<>();
            final Map<String, List<BatchedTask>> processTasksBySource = new HashMap<>();
            final Set<BatchedTask> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);
            if (pending != null) {
                // pending is a java.util.Collections.SynchronizedSet so we can safely iterate holding its mutex
                synchronized (pending) {
                    for (BatchedTask task : pending) {
                        if (task.processed.getAndSet(true) == false) {
                            logger.trace("will process {}", task);
                            toExecute.add(task);
                            processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
                        } else {
                            logger.trace("skipping {}, already processed", task);
                        }
                    }
                }
            }

            if (toExecute.isEmpty() == false) {
                run(updateTask.batchingKey, toExecute, buildTasksDescription(updateTask, toExecute, processTasksBySource));
            }
        }
    }

    private static final int MAX_TASK_DESCRIPTION_CHARS = 8 * 1024;

    private String buildTasksDescription(
        BatchedTask updateTask,
        List<BatchedTask> toExecute,
        Map<String, List<BatchedTask>> processTasksBySource
    ) {
        final StringBuilder output = new StringBuilder();
        Strings.collectionToDelimitedStringWithLimit((Iterable<String>) () -> processTasksBySource.entrySet().stream().map(entry -> {
            String tasks = updateTask.describeTasks(entry.getValue());
            return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
        }).filter(s -> s.isEmpty() == false).iterator(), ", ", "", "", MAX_TASK_DESCRIPTION_CHARS, output);
        if (output.length() > MAX_TASK_DESCRIPTION_CHARS) {
            output.append(" (").append(toExecute.size()).append(" tasks in total)");
        }
        return output.toString();
    }

    /**
     * Action to be implemented by the specific batching implementation
     * All tasks have the given batching key.
     */
    protected abstract void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary);

    /**
     * Represents a runnable task that supports batching.
     * Implementors of TaskBatcher can subclass this to add a payload to the task.
     */
    protected abstract class BatchedTask extends SourcePrioritizedRunnable {
        /**
         * whether the task has been processed already
         */
        protected final AtomicBoolean processed = new AtomicBoolean();

        /**
         * the object that is used as batching key
         */
        protected final Object batchingKey;
        /**
         * the task object that is wrapped
         */
        protected final Object task;

        protected BatchedTask(Priority priority, String source, Object batchingKey, Object task) {
            super(priority, source);
            this.batchingKey = batchingKey;
            this.task = task;
        }

        @Override
        public void run() {
            runIfNotProcessed(this);
        }

        @Override
        public String toString() {
            String taskDescription = describeTasks(Collections.singletonList(this));
            if (taskDescription.isEmpty()) {
                return "[" + source + "]";
            } else {
                return "[" + source + "[" + taskDescription + "]]";
            }
        }

        public abstract String describeTasks(List<? extends BatchedTask> tasks);

        public Object getTask() {
            return task;
        }
    }
}
