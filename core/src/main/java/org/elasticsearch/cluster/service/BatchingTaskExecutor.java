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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Task executor that supports batching for tasks that share the same executor (see {@link BatchingTask#executor})
 */
public abstract class BatchingTaskExecutor extends SingleTaskExecutor {

    final Map<Object, LinkedHashSet<BatchingTask>> tasksPerExecutor = new HashMap<>();

    protected BatchingTaskExecutor(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor, ThreadPool threadPool) {
        super(logger, threadExecutor, threadPool);
    }

    public void submitTasks(List<? extends BatchingTask> tasks, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchingTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.executor == firstTask.executor) :
            "tasks submitted in a batch should share the same executor: " + tasks;
        // convert to an identity map to check for dups based on identity of wrapped task
        final Map<Object, BatchingTask> tasksIdentity = tasks.stream().collect(Collectors.toMap(
            BatchingTask::getWrappedTask,
            Function.identity(),
            (a, b) -> { throw new IllegalStateException("cannot add duplicate task: " + a); },
            IdentityHashMap::new));

        synchronized (tasksPerExecutor) {
            LinkedHashSet<BatchingTask> existingTasks = tasksPerExecutor.computeIfAbsent(firstTask.executor,
                k -> new LinkedHashSet<>(tasks.size()));
            for (BatchingTask existing : existingTasks) {
                // check that there won't be two tasks with the same identity for the same executor
                BatchingTask duplicateTask = tasksIdentity.get(existing.getWrappedTask());
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

    @Override
    public void submitTask(SingleTask task, @Nullable TimeValue timeout) throws EsRejectedExecutionException {
        throw new UnsupportedOperationException();
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
                Object batchingExecutor = toRemove.get(0).executor;
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

    void runIfNotProcessed(BatchingTask updateTask) {
        // if this task is already processed, the executor shouldn't execute other tasks (that arrived later),
        // to give other executors a chance to execute their tasks.
        if (updateTask.processed.get() == false) {
            final List<BatchingTask> toExecute = new ArrayList<>();
            final Map<String, List<BatchingTask>> processTasksBySource = new HashMap<>();
            synchronized (tasksPerExecutor) {
                LinkedHashSet<BatchingTask> pending = tasksPerExecutor.remove(updateTask.executor);
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

                run(updateTask.executor, toExecute, tasksSummary);
            }
        }
    }

    @Override
    protected void run(SingleTask task) {
        throw new UnsupportedOperationException();
    }

    /**
     * Action to be implemented by the specific task executor
     */
    protected abstract void run(Object executor, List<? extends BatchingTask> tasks, String tasksSummary);

    /**
     * Represents a runnable task that supports batching.
     * Implementors of BatchingTaskExecutor can subclass this to add a payload to the task.
     */
    protected abstract class BatchingTask extends SingleTask {
        /**
         * the executor object that is used as batching key
         */
        protected final Object executor;
        /**
         * the task object that is wrapped
         */
        protected final Object wrappedTask;

        protected BatchingTask(Priority priority, String source, Object executor, Object wrappedTask) {
            super(priority, source);
            this.executor = executor;
            this.wrappedTask = wrappedTask;
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

        public abstract String describeTasks(List<? extends BatchingTask> tasks);

        public Object getWrappedTask() {
            return wrappedTask;
        }
    }
}
