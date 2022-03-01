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

        // [HISTORICAL NOTE] In the past, tasks executed by the master service would automatically be notified of acks if they implemented
        // the ClusterStateAckListener interface (the interface formerly known as AckedClusterStateTaskListener). This implicit behaviour
        // was a little troublesome and was removed in favour of having the executor explicitly register an ack listener (where necessary)
        // for each task it successfully executes. Making this change carried the risk that someone might implement a new task in the future
        // which relied on the old implicit behaviour based on the interfaces that the task implements instead of the explicit behaviour in
        // the executor. We protect against this with some weird-looking assertions in the success() methods below which insist that
        // ack-listening tasks register themselves as their own ack listener. If you want to supply a different ack listener then you must
        // remove the ClusterStateAckListener interface from the task to make it clear that the task itself is not expecting to be notified
        // of acks.
        //
        // Note that the old implicit behaviour lives on in the unbatched() executor so that it can correctly execute either a
        // ClusterStateUpdateTask or an AckedClusterStateUpdateTask.

        public static class Builder<T extends ClusterStateTaskListener> {
            private final Map<T, TaskResult> executionResults = new IdentityHashMap<>();

            /**
             * Record that the cluster state update task succeeded.
             *
             *
             * @param task The task that succeeded. Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks
             *             themselves. If so, you may not use this method and must instead call {@link #success(ClusterStateTaskListener,
             *             ActionListener, ClusterStateAckListener)}, passing the task itself as the {@code clusterStateAckListener}
             *             argument.
             *
             * @param publishListener A listener for the completion of the resulting cluster state publication. This listener is completed
             *                        with the cluster state that was published (or the publication exception that occurred) in the thread
             *                        context in which the task was submitted. The task's {@link
             *                        ClusterStateTaskListener#clusterStateProcessed} method is not called directly by the master service,
             *                        nor is {@link ClusterStateTaskListener#onFailure} once the task execution has succeeded, but legacy
             *                        implementations may use this listener to call those methods.
             *                        <p>
             *                        The listener should prefer not to use the published state for things like determining the result of a
             *                        task. The task may have been executed as part of a batch, and later tasks in the batch may overwrite
             *                        the results from earlier tasks. Instead the listener should independently capture the information it
             *                        needs to properly process the completion of a cluster state update.
             */
            // TODO remove all remaining usages of the published state and then make publishListener an ActionListener<Void>
            public Builder<T> success(T task, ActionListener<ClusterState> publishListener) {
                assert task instanceof ClusterStateAckListener == false // see [HISTORICAL NOTE] above
                    : "tasks that implement ClusterStateAckListener must explicitly supply themselves as the ack listener";
                return result(task, TaskResult.success(publishListener));
            }

            /**
             * Record that the cluster state update task succeeded.
             *
             * @param task The task that succeeded. Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks
             *             themselves. If so, you must pass the task itself as the {@code clusterStateAckListener} argument.
             *
             * @param publishListener A listener for the completion of the resulting cluster state publication. This listener is completed
             *                        with the cluster state that was published (or the publication exception that occurred) in the thread
             *                        context in which the task was submitted. The task's {@link
             *                        ClusterStateTaskListener#clusterStateProcessed} method is not called directly by the master service,
             *                        nor is {@link ClusterStateTaskListener#onFailure} once the task execution has succeeded, but legacy
             *                        implementations may use this listener to call those methods.
             *                        <p>
             *                        The listener should prefer not to use the published state for things like determining the result of a
             *                        task. The task may have been executed as part of a batch, and later tasks in the batch may overwrite
             *                        the results from earlier tasks. Instead the listener should independently capture the information it
             *                        needs to properly process the completion of a cluster state update.
             *
             * @param clusterStateAckListener A listener for acknowledgements from nodes. If the publication succeeds then this listener is
             *                                completed as nodes ack the state update. If the publication fails then the failure
             *                                notification happens via {@code publishListener.onFailure()}: this listener is not notified.
             */
            // TODO remove all remaining usages of the published state and then make publishListener an ActionListener<Void>
            public Builder<T> success(
                T task,
                ActionListener<ClusterState> publishListener,
                ClusterStateAckListener clusterStateAckListener
            ) {
                assert task == clusterStateAckListener || task instanceof ClusterStateAckListener == false // see [HISTORICAL NOTE] above
                    : "tasks that implement ClusterStateAckListener must not supply a separate clusterStateAckListener";
                return result(task, TaskResult.success(publishListener, clusterStateAckListener));
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

    /**
     * @param publishListener         Listener supplied by a successfully-executed task which will be completed at the end of publication.
     * @param clusterStateAckListener Listener optionally supplied by a successfully-executed task which will be completed after acking.
     * @param failure                 Exception supplied by an unsuccessfully-executed task.
     */
    record TaskResult(
        @Nullable ActionListener<ClusterState> publishListener,
        @Nullable ClusterStateAckListener clusterStateAckListener,
        @Nullable Exception failure
    ) {

        public TaskResult {
            assert failure == null ^ publishListener == null;
            assert clusterStateAckListener == null || failure == null;
        }

        public static TaskResult success(ActionListener<ClusterState> publishListener) {
            return new TaskResult(Objects.requireNonNull(publishListener), null, null);
        }

        public static TaskResult success(ActionListener<ClusterState> publishListener, ClusterStateAckListener clusterStateAckListener) {
            return new TaskResult(Objects.requireNonNull(publishListener), Objects.requireNonNull(clusterStateAckListener), null);
        }

        public static TaskResult failure(Exception failure) {
            return new TaskResult(null, null, Objects.requireNonNull(failure));
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
     * <p>
     * If the task to be executed also implements {@link ClusterStateAckListener} then it is notified on acks.
     */
    static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> unbatched() {
        return new ClusterStateTaskExecutor<>() {
            @Override
            public ClusterTasksResult<T> execute(ClusterState currentState, List<T> tasks) throws Exception {
                assert tasks.size() == 1 : "this only supports a single task but received " + tasks;
                final T task = tasks.get(0);
                final var newState = task.execute(currentState);
                final var builder = ClusterTasksResult.<T>builder();
                final var publishListener = new ActionListener<ClusterState>() {
                    @Override
                    public void onResponse(ClusterState publishedState) {
                        task.clusterStateProcessed(currentState, publishedState);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        task.onFailure(e);
                    }
                };
                if (task instanceof ClusterStateAckListener ackListener) {
                    builder.success(task, publishListener, ackListener);
                } else {
                    builder.success(task, publishListener);
                }
                return builder.build(newState);
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
