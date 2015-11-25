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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public interface ClusterStateTaskExecutor<T> {
    /**
     * Update the cluster state based on the current state and the given tasks. Return the *same instance* if no state
     * should be changed.
     */
    Result<T> execute(ClusterState currentState, List<T> tasks) throws Exception;

    /**
     * indicates whether this task should only run if current node is master
     */
    default boolean runOnlyOnMaster() {
        return true;
    }

    /**
     * Represents the result of a batched execution of cluster state update tasks
     * @param <T> the type of the cluster state update task
     */
    class Result<T> {
        final public ClusterState resultingState;
        final public Map<T, ClusterStateTaskExecutionResult> executionResults;

        /**
         * Construct an execution result instance for which every cluster state update task succeeded
         * @param resultingState the resulting cluster state
         * @param tasks the cluster state update tasks
         */
        public Result(ClusterState resultingState, List<T> tasks) {
            this(resultingState, tasks.stream().collect(Collectors.toMap(task -> task, task -> ClusterStateTaskExecutionResult.success())));
        }

        /**
         * Construct an execution result instance with a correspondence between the tasks and their execution result
         * @param resultingState the resulting cluster state
         * @param executionResults the correspondence between tasks and their outcome
         */
        public Result(ClusterState resultingState, Map<T, ClusterStateTaskExecutionResult> executionResults) {
            this.resultingState = resultingState;
            this.executionResults = executionResults;
        }
    }

    final class ClusterStateTaskExecutionResult {
        private final Throwable failure;

        private static final ClusterStateTaskExecutionResult SUCCESS = new ClusterStateTaskExecutionResult(null);

        public static ClusterStateTaskExecutionResult success() {
            return SUCCESS;
        }

        public static ClusterStateTaskExecutionResult failure(Throwable failure) {
            return new ClusterStateTaskExecutionResult(failure);
        }

        private ClusterStateTaskExecutionResult(Throwable failure) {
            this.failure = failure;
        }

        public boolean isSuccess() {
            return failure != null;
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
