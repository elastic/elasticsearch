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

import java.util.List;

/**
 * An executor for batches of cluster state update tasks.
 *
 * @param <T> The type of tasks to execute.
 */
public interface ClusterStateTaskExecutor<T extends ClusterStateTaskListener> {
    /**
     * Update the cluster state based on the current state and the given tasks. Return the *same instance* if no update should be published.
     * <p>
     * If this method throws an exception then the cluster state is unchanged and every task's {@link ClusterStateTaskListener#onFailure}
     * method is called.
     * <p>
     * A common implementation pattern is to iterate through the tasks, constructing a new and updated {@link ClusterState} for each one.
     * This works ok but beware that constructing a whole new {@link ClusterState} can be somewhat expensive, and there may sometimes be
     * surprisingly many tasks to process in the batch. If it's possible to accumulate the effects of the tasks at a lower level then you
     * should do that instead.
     *
     * @param currentState The initial cluster state on which the tasks should be executed.
     * @param taskContexts A {@link TaskContext} for each task in the batch. Implementations must complete every context in the list.
     * @return The resulting cluster state after executing all the tasks. If {code currentState} is returned then no update is published.
     */
    ClusterState execute(ClusterState currentState, List<TaskContext<T>> taskContexts) throws Exception;

    /**
     * @return {@code true} iff this executor should only run on the elected master.
     */
    default boolean runOnlyOnMaster() {
        return true;
    }

    /**
     * Callback invoked after new cluster state is published. Note that this method is not invoked if the cluster state was not updated.
     *
     * Note that this method will be executed using system context.
     *
     * @param newClusterState The new state which was published.
     */
    default void clusterStatePublished(ClusterState newClusterState) {}

    /**
     * Builds a concise description of a list of tasks (to be used in logging etc.).
     *
     * Note that the tasks given are not necessarily the same as those that will be passed to {@link #execute(ClusterState, List)}.
     * but are guaranteed to be a subset of them. This method can be called multiple times with different lists before execution.
     *
     * @param tasks the tasks to describe.
     * @return A string which describes the batch of tasks.
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
     * Creates a task executor that only executes a single task. Use a new instance of this executor to specifically submit a cluster state
     * update task that should be executed in isolation and not be batched with other state updates.
     * <p>
     * This executor exists for legacy reasons but is forbidden in new production code because unbatched tasks are a source of performance
     * and stability bugs. You should instead implement your update logic in a dedicated {@link ClusterStateTaskExecutor} which is reused
     * across multiple task instances. The task itself is typically just a collection of parameters consumed by the executor, together with
     * any listeners to be notified when execution completes.
     *
     * @param <T> The type of task to execute
     * @return A single-use executor to execute a single task.
     */
    static <T extends ClusterStateUpdateTask> ClusterStateTaskExecutor<T> unbatched() {
        return new ClusterStateTaskExecutor<>() {
            @Override
            public ClusterState execute(ClusterState currentState, List<TaskContext<T>> taskContexts) throws Exception {
                assert taskContexts.size() == 1 : "this only supports a single task but received " + taskContexts;
                final var taskContext = taskContexts.get(0);
                final var task = taskContext.getTask();
                final var newState = task.execute(currentState);
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
                    taskContext.success(publishListener, ackListener);
                } else {
                    taskContext.success(publishListener);
                }
                return newState;
            }

            @Override
            public String describeTasks(List<T> tasks) {
                return ""; // one of task, source is enough
            }
        };
    }

    /**
     * An {@link ActionListener} for passing to {@link ClusterStateTaskExecutor.TaskContext#success} which preserves the
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

    /**
     * A task to be executed, along with callbacks for the executor to record the outcome of this task's execution. The executor must
     * call exactly one of these methods for every task in its batch.
     */
    interface TaskContext<T extends ClusterStateTaskListener> {

        /**
         * @return the task to be executed.
         */
        T getTask();

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you may not use this
         * method and must instead call {@link #success(ActionListener, ClusterStateAckListener)}, passing the task itself as the {@code
         * clusterStateAckListener} argument.
         *
         * @param publishListener A listener for the completion of the resulting cluster state publication. This listener is completed
         *                        with the cluster state that was published (or the publication exception that occurred) in the thread
         *                        context in which the task was submitted. The task's {@link
         *                        ClusterStateTaskListener#clusterStateProcessed} method is not called directly by the master service,
         *                        nor is {@link ClusterStateTaskListener#onFailure} once the task execution has succeeded, but legacy
         *                        implementations may supply a listener which calls those methods.
         *                        <p>
         *                        The listener should prefer not to use the published state for things like determining the result of a
         *                        task. The task may have been executed as part of a batch, and later tasks in the batch may overwrite
         *                        the results from earlier tasks. Instead the listener should independently capture the information it
         *                        needs to properly process the completion of a cluster state update.
         */
        // TODO remove all remaining usages of the published state and then make publishListener an ActionListener<Void>
        // see https://github.com/elastic/elasticsearch/issues/84415
        void success(ActionListener<ClusterState> publishListener);

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you must pass the task
         * itself as the {@code clusterStateAckListener} argument.
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
        // see https://github.com/elastic/elasticsearch/issues/84415
        void success(ActionListener<ClusterState> publishListener, ClusterStateAckListener clusterStateAckListener);

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you must pass the task
         * itself as the {@code clusterStateAckListener} argument.
         * <p>
         * This method is useful in cases where the task will take some action at the end of acking but takes no action at the end of
         * publication. If publication fails then the task's {@link ClusterStateTaskListener#onFailure} method is called.
         *
         * @param clusterStateAckListener A listener for acknowledgements from nodes. If the publication succeeds then this listener is
         *                                completed as nodes ack the state update. If the publication fails then the failure
         *                                notification happens via {@code publishListener.onFailure()}: this listener is not notified.
         */
        default void success(ClusterStateAckListener clusterStateAckListener) {
            success(new SuccessIgnoringPublishListener(getTask()), clusterStateAckListener);
        }

        /**
         * Record that the cluster state update task failed.
         *
         * @param failure The exception with which the task failed.
         */
        void onFailure(Exception failure);
    }

    /**
     * Tasks that listen for acks typically do not care about a successfully-completed publication, but must still handle publication
     * failures. This adapter makes that pattern easy.
     */
    record SuccessIgnoringPublishListener(ClusterStateTaskListener task) implements ActionListener<ClusterState> {
        @Override
        public void onResponse(ClusterState clusterState) {
            // nothing to do here, listener is completed at the end of acking
        }

        @Override
        public void onFailure(Exception e) {
            task.onFailure(e);
        }
    }
}
