/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    final ConcurrentMap<Object, Map<IdentityWrapper, BatchedTask>> tasksPerBatchingKey = new ConcurrentHashMap<>();

    public TaskBatcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
    }

    public void submitTasks(List<? extends BatchedTask> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchedTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey) :
            "tasks submitted in a batch should share the same batching key: " + tasks;
        // convert to an identity map to check for dups based on task identity
        final Map<IdentityWrapper, BatchedTask> toAdd = tasks.stream().collect(Collectors.toMap(
            t -> new IdentityWrapper(t.getTask()),
            Function.identity(),
            (a, b) -> { throw new IllegalStateException("cannot add duplicate task: " + a); },
            LinkedHashMap::new));

        tasksPerBatchingKey.merge(
            firstTask.batchingKey,
            toAdd,
            (oldValue, newValue) -> {
                final Map<IdentityWrapper, BatchedTask> merged =
                    new LinkedHashMap<>(oldValue.size() + newValue.size());
                merged.putAll(oldValue);
                merged.putAll(newValue);

                if (merged.size() != oldValue.size() + newValue.size()) {
                    // Find the duplicate
                    oldValue.forEach((k, existing) -> {
                        final BatchedTask duplicateTask = newValue.get(k);
                        if (duplicateTask != null) {
                            throw new IllegalStateException("task [" + duplicateTask.describeTasks(
                                Collections.singletonList(existing)) + "] with source [" + duplicateTask.source + "] is already queued");
                        }
                    });
                }
                return merged;
            });

        if (timeout != null) {
            threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    private void onTimeoutInternal(List<? extends BatchedTask> tasks, TimeValue timeout) {
        final Set<IdentityWrapper> ids = new HashSet<>(tasks.size());
        final List<BatchedTask> toRemove = new ArrayList<>(tasks.size());
        for (BatchedTask task : tasks) {
            if (!task.processed.getAndSet(true)) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                ids.add(new IdentityWrapper(task.getTask()));
                toRemove.add(task);
            }
        }

        if (toRemove.isEmpty()) {
            return;
        }

        final BatchedTask firstTask = toRemove.get(0);
        final Object batchingKey = firstTask.batchingKey;
        assert tasks.stream().allMatch(t -> t.batchingKey == batchingKey) :
            "tasks submitted in a batch should share the same batching key: " + tasks;
        tasksPerBatchingKey.computeIfPresent(
            batchingKey,
            (k, v) -> {
                if (v.size() == ids.size() && ids.containsAll(v.keySet())) {
                    // Special case when all the tasks timed out
                    return null;
                } else {
                    final Map<IdentityWrapper, BatchedTask> merged = new LinkedHashMap<>(v.size());
                    v.forEach((id, task) -> {
                        if (!ids.contains(id)) {
                            merged.put(id, task);
                        }
                    });
                    return merged;
                }
            });

        onTimeout(toRemove, timeout);
    }

    /**
     * Action to be implemented by the specific batching implementation.
     * All tasks have the same batching key.
     */
    protected abstract void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout);

    private void runIfNotProcessed(BatchedTask updateTask) {
        // if this task is already processed, it shouldn't execute other tasks with same batching key that arrived later,
        // to give other tasks with different batching key a chance to execute.
        if (!updateTask.processed.get()) {
            final List<BatchedTask> toExecute = new ArrayList<>();
            final Map<String, List<BatchedTask>> processTasksBySource = new HashMap<>();
            final Map<IdentityWrapper, BatchedTask> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);
            if (pending != null) {
                for (BatchedTask task : pending.values()) {
                    if (!task.processed.getAndSet(true)) {
                        logger.trace("will process {}", task);
                        toExecute.add(task);
                        processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
                    } else {
                        logger.trace("skipping {}, already processed", task);
                    }
                }
            }

            if (!toExecute.isEmpty()) {
                final String tasksSummary = processTasksBySource.entrySet().stream().map(entry -> {
                    String tasks = updateTask.describeTasks(entry.getValue());
                    return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");

                run(updateTask.batchingKey, toExecute, tasksSummary);
            }
        }
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

    /**
     * Uses wrapped {@link Object} identity for {@link #equals(Object)} and {@link #hashCode()}.
     */
    private static final class IdentityWrapper {
        private final Object object;

        private IdentityWrapper(final Object object) {
            this.object = object;
        }

        @Override
        public boolean equals(final Object o) {
            assert o instanceof IdentityWrapper;
            final IdentityWrapper that = (IdentityWrapper) o;
            return object == that.object;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(object);
        }
    }
}
