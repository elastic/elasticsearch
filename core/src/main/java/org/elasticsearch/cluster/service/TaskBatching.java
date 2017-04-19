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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Batching support for {@link PrioritizedEsThreadPoolExecutor}
 * Tasks that share the same batching key are batched (see {@link BatchingTask#batchingKey})
 */
public abstract class TaskBatching {

    protected final Logger logger;
    protected final ThreadPool threadPool;
    protected final PrioritizedEsThreadPoolExecutor threadExecutor;
    final Map<Object, LinkedHashSet<BatchingTask>> tasksPerExecutor = new HashMap<>();

    public TaskBatching(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor, ThreadPool threadPool) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
        this.threadPool = threadPool;
    }

    public void submitTasks(List<? extends BatchingTask> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchingTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey) :
            "tasks submitted in a batch should share the same batching key: " + tasks;
        // convert to an identity map to check for dups based on task identity
        final Map<Object, BatchingTask> tasksIdentity = tasks.stream().collect(Collectors.toMap(
            BatchingTask::getTaskIdentity,
            Function.identity(),
            (a, b) -> { throw new IllegalStateException("cannot add duplicate task: " + a); },
            IdentityHashMap::new));

        synchronized (tasksPerExecutor) {
            LinkedHashSet<BatchingTask> existingTasks = tasksPerExecutor.computeIfAbsent(firstTask.batchingKey,
                k -> new LinkedHashSet<>(tasks.size()));
            for (BatchingTask existing : existingTasks) {
                // check that there won't be two tasks with the same identity for the same batching key
                BatchingTask duplicateTask = tasksIdentity.get(existing.getTaskIdentity());
                if (duplicateTask != null) {
                    throw new IllegalStateException("task [" + duplicateTask.describeTasks(
                        Collections.singletonList(existing)) + "] with source [" + duplicateTask.source + "] is already queued");
                }
            }
            existingTasks.addAll(tasks);
        }

        if (timeout != null) {
            threadExecutor.execute(firstTask, threadPool.scheduler(), timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    private void onTimeoutInternal(List<? extends BatchingTask> updateTasks, TimeValue timeout) {
        threadPool.generic().execute(() -> {
            final ArrayList<BatchingTask> toRemove = new ArrayList<>();
            for (BatchingTask task : updateTasks) {
                if (task.processed.getAndSet(true) == false) {
                    logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                    toRemove.add(task);
                }
            }
            if (toRemove.isEmpty() == false) {
                Object batchingExecutor = toRemove.get(0).batchingKey;
                synchronized (tasksPerExecutor) {
                    LinkedHashSet<BatchingTask> existingTasks = tasksPerExecutor.get(batchingExecutor);
                    if (existingTasks != null) {
                        existingTasks.removeAll(toRemove);
                        if (existingTasks.isEmpty()) {
                            tasksPerExecutor.remove(batchingExecutor);
                        }
                    }
                }
                for (BatchingTask task : toRemove) {
                    onTimeout(task, timeout);
                }
            }
        });
    }

    /**
     * Action to be implemented by the specific batching implementation
     */
    protected abstract void onTimeout(BatchingTask task, TimeValue timeout);

    void runIfNotProcessed(BatchingTask updateTask) {
        // if this task is already processed, the executor shouldn't execute other tasks (that arrived later),
        // to give other executors a chance to execute their tasks.
        if (updateTask.processed.get() == false) {
            final List<BatchingTask> toExecute = new ArrayList<>();
            final Map<String, List<BatchingTask>> processTasksBySource = new HashMap<>();
            synchronized (tasksPerExecutor) {
                LinkedHashSet<BatchingTask> pending = tasksPerExecutor.remove(updateTask.batchingKey);
                if (pending != null) {
                    for (BatchingTask task : pending) {
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
     */
    protected abstract void run(Object batchingKey, List<? extends BatchingTask> tasks, String tasksSummary);

    /**
     * Represents a runnable task that supports batching.
     * Implementors of TaskBatching can subclass this to add a payload to the task.
     */
    protected abstract class BatchingTask extends SourcePrioritizedRunnable {
        /**
         * whether the task has been processed already
         */
        protected final AtomicBoolean processed = new AtomicBoolean();

        /**
         * the object that is used as batching key
         */
        protected final BatchingKey<?> batchingKey;
        /**
         * the task object that is wrapped
         */
        protected final Object taskIdentity;

        protected BatchingTask(Priority priority, String source, BatchingKey<?> batchingKey, Object taskIdentity) {
            super(priority, source);
            this.batchingKey = batchingKey;
            this.taskIdentity = taskIdentity;
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

        @SuppressWarnings("unchecked")
        public String describeTasks(List<? extends BatchingTask> tasks) {
            return ((BatchingKey) batchingKey).describeTasks(
                tasks.stream().map(BatchingTask::getTaskIdentity).collect(Collectors.toList()));
        }

        public Object getTaskIdentity() {
            return taskIdentity;
        }
    }

    public interface BatchingKey<T> {
        /**
         * Builds a concise description of a list of tasks (to be used in logging etc.).
         *
         * This method can be called multiple times with different lists before execution.
         * This allows groupd task description but the submitting source.
         */
        default String describeTasks(List<T> tasks) {
            return tasks.stream().map(T::toString).reduce((s1, s2) -> {
                if (s1.isEmpty()) {
                    return s2;
                } else if (s2.isEmpty()) {
                    return s1;
                } else {
                    return s1 + ", " + s2;
                }
            }).orElse("");
        }
    }
}
