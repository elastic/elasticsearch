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
package org.elasticsearch.cluster;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface ClusterStateTaskExecutor<T> {
    /**
     * Update the cluster state based on the current state and the given tasks. Return the *same instance* if no state
     * should be changed.
     */
    BatchResult<T> execute(ClusterState currentState, List<T> tasks) throws Exception;

    /**
     * indicates whether this task should only run if current node is master
     */
    default boolean runOnlyOnMaster() {
        return true;
    }

    /**
     * Callback invoked after new cluster state is published. Note that
     * this method is not invoked if the cluster state was not updated.
     * @param clusterChangedEvent the change event for this cluster state change, containing
     *                            both old and new states
     */
    default void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
    }

    /**
     * Represents the result of a batched execution of cluster state update tasks
     * @param <T> the type of the cluster state update task
     */
    class BatchResult<T> {
        final public ClusterState resultingState;
        final public Map<T, TaskResult> executionResults;

        /**
         * Construct an execution result instance with a correspondence between the tasks and their execution result
         * @param resultingState the resulting cluster state
         * @param executionResults the correspondence between tasks and their outcome
         */
        BatchResult(ClusterState resultingState, Map<T, TaskResult> executionResults) {
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

            public Builder<T> failure(T task, Throwable t) {
                return result(task, TaskResult.failure(t));
            }

            public Builder<T> failures(Iterable<T> tasks, Throwable t) {
                for (T task : tasks) {
                    failure(task, t);
                }
                return this;
            }

            private Builder<T> result(T task, TaskResult executionResult) {
                TaskResult existing = executionResults.put(task, executionResult);
                assert existing == null : task + " already has result " + existing;
                return this;
            }

            public BatchResult<T> build(ClusterState resultingState) {
                return new BatchResult<>(resultingState, executionResults);
            }
        }
    }

    final class TaskResult {
        private final Throwable failure;

        private static final TaskResult SUCCESS = new TaskResult(null);

        public static TaskResult success() {
            return SUCCESS;
        }

        public static TaskResult failure(Throwable failure) {
            return new TaskResult(failure);
        }

        private TaskResult(Throwable failure) {
            this.failure = failure;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public Throwable getFailure() {
            assert !isSuccess();
            return failure;
        }

        /**
         * Handle the execution result with the provided consumers
         * @param onSuccess handler to invoke on success
         * @param onFailure handler to invoke on failure; the throwable passed through will not be null
         */
        public void handle(Runnable onSuccess, Consumer<Throwable> onFailure) {
            if (failure == null) {
                onSuccess.run();
            } else {
                onFailure.accept(failure);
            }
        }
    }
}
