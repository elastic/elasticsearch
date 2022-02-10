/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface ClusterStateTaskExecutor<T extends ClusterStateTaskListener> {
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
     * @param clusterStatePublicationEvent the change event for this cluster state publication, containing both old and new states
     */
    default void clusterStatePublished(ClusterStatePublicationEvent clusterStatePublicationEvent) {}

    /**
     * Builds a concise description of a list of tasks (to be used in logging etc.).
     *
     * Note that the tasks given are not necessarily the same as those that will be passed to {@link #execute(ClusterState, List)}.
     * but are guaranteed to be a subset of them. This method can be called multiple times with different lists before execution.
     * This allows groupd task description but the submitting source.
     */
    default String describeTasks(List<T> tasks) {
        final StringBuilder output = new StringBuilder();
        Strings.collectionToDelimitedStringWithLimit(
            (Iterable<String>) () -> tasks.stream().map(Object::toString).filter(s -> s.isEmpty() == false).iterator(),
            ", ",
            "",
            "",
            1024,
            output
        );
        return output.toString();
    }

    /**
     * Represents the result of a batched execution of cluster state update tasks
     *
     * @param <T> the type of the cluster state update task
     */
    record ClusterTasksResult<T extends ClusterStateTaskListener> (
        @Nullable ClusterState resultingState, // the resulting cluster state
        Map<T, TaskResult> executionResults    // the correspondence between tasks and their outcome
    ) {

        public static <T extends ClusterStateTaskListener> Builder<T> builder() {
            return new Builder<>();
        }

        public static class Builder<T extends ClusterStateTaskListener> {
            private final Map<T, TaskResult> executionResults = new IdentityHashMap<>();

            /**
             * Record that the cluster state update task succeeded.
             *
             * @param taskListener A listener for the completion of the resulting cluster state publication. This listener is completed with
             *                     the cluster state that was published (or the publication exception that occurred) in the thread context
             *                     in which the task was submitted. The task's {@link ClusterStateTaskListener#clusterStateProcessed} method
             *                     is not called directly by the master service, nor is {@link ClusterStateTaskListener#onFailure} once the
             *                     task execution has succeeded, but legacy implementations may use this listener to call those methods.
             *                     <p>
             *                     The listener should prefer not to use the published state for things like determining the result of a
             *                     task. The task may have been executed as part of a batch, and later tasks in the batch may overwrite
             *                     the results from earlier tasks. Instead the listener should independently capture the information it
             *                     needs to properly process the completion of a cluster state update.
             */
            // TODO remove all remaining usages of the published state and then make this an ActionListener<Void>
            public Builder<T> success(T task, ActionListener<ClusterState> taskListener) {
                return result(task, TaskResult.success(taskListener));
            }

            /**
             * Record that the cluster state update task failed.
             */
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
        }
    }

    record TaskResult(@Nullable ActionListener<ClusterState> taskListener, @Nullable Exception failure) {

        public TaskResult {
            assert failure == null ^ taskListener == null;
        }

        public static TaskResult success(ActionListener<ClusterState> taskListener) {
            return new TaskResult(Objects.requireNonNull(taskListener), null);
        }

        public static TaskResult failure(Exception failure) {
            return new TaskResult(null, Objects.requireNonNull(failure));
        }

        public boolean isSuccess() {
            return failure == null;
        }

        public Exception getFailure() {
            assert isSuccess() == false;
            return failure;
        }
    }

    /**
     * Creates a task executor that only executes a single task. Use a new instance of this executor to specifically submit a cluster state
     * update task that should be executed in isolation and not be batched with other state updates.
     */
    static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> unbatched() {
        return new ClusterStateTaskExecutor<>() {
            @Override
            public ClusterTasksResult<T> execute(ClusterState currentState, List<T> tasks) throws Exception {
                assert tasks.size() == 1 : "this only supports a single task but received " + tasks;
                final T task = tasks.get(0);
                final ClusterState newState = task.execute(currentState);
                return ClusterTasksResult.<T>builder().success(task, new ActionListener<>() {
                    @Override
                    public void onResponse(ClusterState publishedState) {
                        task.clusterStateProcessed(currentState, publishedState);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        task.onFailure(e);
                    }
                }).build(newState);
            }

            @Override
            public String describeTasks(List<T> tasks) {
                return ""; // one of task, source is enough
            }
        };
    }

    /**
     * An {@link ActionListener} for passing to {@link ClusterStateTaskExecutor.ClusterTasksResult.Builder#success} which preserves the
     * legacy behaviour of calling {@link ClusterStateTaskListener#clusterStateProcessed} or {@link ClusterStateTaskListener#onFailure}.
     * <p>
     * New implementations should use a dedicated listener rather than relying on this legacy behaviour.
     */
    // TODO remove all remaining usages of this listener
    record LegacyClusterTaskResultActionListener(ClusterStateTaskListener task, ClusterState originalState)
        implements
            ActionListener<ClusterState> {

        @Override
        public void onResponse(ClusterState publishedState) {
            task.clusterStateProcessed(originalState, publishedState);
        }

        @Override
        public void onFailure(Exception e) {
            task.onFailure(e);
        }
    }

}
