/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.core.Nullable;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public interface ClusterStateTaskExecutor<T> {
    /**
     * Update the cluster state based on the current state and the given tasks. Return the *same instance* if no state
     * should be changed.
     */
    ClusterTasksResult<T> execute(ClusterState currentState, List<T> tasks) throws Exception;

    /**
     * indicates whether this executor should only run if the current node is master
     */
    default boolean runOnlyOnMaster() {
        return true;
    }

    /**
     * Callback invoked after new cluster state is published. Note that
     * this method is not invoked if the cluster state was not updated.
     *
     * Note that this method will be executed using system context.
     *
     * @param clusterChangedEvent the change event for this cluster state change, containing
     *                            both old and new states
     */
    default void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
    }

    /**
     * Builds a concise description of a list of tasks (to be used in logging etc.).
     *
     * Note that the tasks given are not necessarily the same as those that will be passed to {@link #execute(ClusterState, List)}.
     * but are guaranteed to be a subset of them. This method can be called multiple times with different lists before execution.
     * This allows groupd task description but the submitting source.
     */
    default String describeTasks(List<T> tasks) {
        return String.join(", ", tasks.stream().map(t -> (CharSequence)t.toString()).filter(t -> t.length() > 0)::iterator);
    }

    /**
     * Represents the result of a batched execution of cluster state update tasks
     * @param <T> the type of the cluster state update task
     */
    class ClusterTasksResult<T> {
        @Nullable
        public final ClusterState resultingState;
        public final Map<T, TaskResult> executionResults;

        /**
         * Construct an execution result instance with a correspondence between the tasks and their execution result
         * @param resultingState the resulting cluster state
         * @param executionResults the correspondence between tasks and their outcome
         */
        ClusterTasksResult(ClusterState resultingState, Map<T, TaskResult> executionResults) {
            this.resultingState = resultingState;
            this.executionResults = executionResults;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        public static class Builder<T> {
            private final Map<T, TaskResult> executionResults = new IdentityHashMap<>();

            public Builder<T> success(T task) {
                return result(task, TaskResult.success());
            }

            public Builder<T> successes(Iterable<T> tasks) {
                for (T task : tasks) {
                    success(task);
                }
                return this;
            }

            public Builder<T> failure(T task, Exception e) {
                return result(task, TaskResult.failure(e));
            }

            public Builder<T> failures(Iterable<T> tasks, Exception e) {
                for (T task : tasks) {
                    failure(task, e);
                }
                return this;
            }

            private Builder<T> result(T task, TaskResult executionResult) {
                TaskResult existing = executionResults.put(task, executionResult);
                assert existing == null : task + " already has result " + existing;
                return this;
            }

            public ClusterTasksResult<T> build(ClusterState resultingState) {
                return new ClusterTasksResult<>(resultingState, executionResults);
            }

            ClusterTasksResult<T> build(ClusterTasksResult<T> result, ClusterState previousState) {
                return new ClusterTasksResult<>(result.resultingState == null ? previousState : result.resultingState,
                    executionResults);
            }
        }
    }

    final class TaskResult {
        private final Exception failure;

        private static final TaskResult SUCCESS = new TaskResult(null);

        public static TaskResult success() {
            return SUCCESS;
        }

        public static TaskResult failure(Exception failure) {
            return new TaskResult(failure);
        }

        private TaskResult(Exception failure) {
            this.failure = failure;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public Exception getFailure() {
            assert isSuccess() == false;
            return failure;
        }
    }
}
