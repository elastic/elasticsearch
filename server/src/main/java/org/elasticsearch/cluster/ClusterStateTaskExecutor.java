/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Releasable;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
     * @return The resulting cluster state after executing all the tasks. If {code initialState} is returned then no update is published.
     */
    ClusterState execute(BatchExecutionContext<T> batchExecutionContext) throws Exception;

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
     * Note that the tasks given are not necessarily the same as those that will be passed to {@link #execute} but are guaranteed to be a
     * subset of them. This method can be called multiple times with different lists before execution.
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
         * method and must instead call {@link #success(Runnable, ClusterStateAckListener)}, passing the task itself as the {@code
         * clusterStateAckListener} argument.
         *
         * @param onPublicationSuccess An action executed when (if?) the cluster state update succeeds.
         */
        void success(Runnable onPublicationSuccess);

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you may not use this
         * method and must instead call {@link #success(Consumer, ClusterStateAckListener)}, passing the task itself as the {@code
         * clusterStateAckListener} argument.
         *
         * @param publishedStateConsumer A consumer of the cluster state that was ultimately published.
         *                               <p>
         *                               The consumer should prefer not to use the published state for things like determining the result
         *                               of a task. The task may have been executed as part of a batch, and later tasks in the batch may
         *                               overwrite the results from earlier tasks. Instead the listener should independently capture the
         *                               information it needs to properly process the completion of a cluster state update.
         */
        // TODO remove all remaining usages of the published state and migrate all callers to the Runnable variant, then remove this
        // see https://github.com/elastic/elasticsearch/issues/84415
        @Deprecated
        void success(Consumer<ClusterState> publishedStateConsumer);

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you must pass the task
         * itself as the {@code clusterStateAckListener} argument.
         *
         * @param onPublicationSuccess An action executed when (if?) the cluster state update succeeds.
         *
         * @param clusterStateAckListener A listener for acknowledgements from nodes. If the publication succeeds then this listener is
         *                                completed as nodes ack the state update. If the publication fails then the failure
         *                                notification happens via {@code publishListener.onFailure()}: this listener is not notified.
         */
        void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener);

        /**
         * Record that the task succeeded.
         * <p>
         * Note that some tasks implement {@link ClusterStateAckListener} and can listen for acks themselves. If so, you must pass the task
         * itself as the {@code clusterStateAckListener} argument.
         *
         * @param publishedStateConsumer A consumer of the cluster state that was ultimately published.
         *                               <p>
         *                               The consumer should prefer not to use the published state for things like determining the result
         *                               of a task. The task may have been executed as part of a batch, and later tasks in the batch may
         *                               overwrite the results from earlier tasks. Instead the listener should independently capture the
         *                               information it needs to properly process the completion of a cluster state update.
         *
         * @param clusterStateAckListener A listener for acknowledgements from nodes. If the publication succeeds then this listener is
         *                                completed as nodes ack the state update. If the publication fails then the failure
         *                                notification happens via {@code publishListener.onFailure()}: this listener is not notified.
         */
        // TODO remove all remaining usages of the published state and migrate all callers to the Runnable variant, then remove this
        // see https://github.com/elastic/elasticsearch/issues/84415
        @Deprecated
        void success(Consumer<ClusterState> publishedStateConsumer, ClusterStateAckListener clusterStateAckListener);

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
            success(() -> {}, clusterStateAckListener);
        }

        /**
         * Record that the cluster state update task failed.
         *
         * @param failure The exception with which the task failed.
         */
        void onFailure(Exception failure);

        /**
         * Creates a context which captures any response headers (e.g. deprecation warnings) to be fed to the task's listener on completion.
         */
        Releasable captureResponseHeaders();
    }

    /**
     * Encapsulates the context in which a batch of tasks executes.
     *
     * @param initialState The initial cluster state on which the tasks should be executed.
     * @param taskContexts A {@link TaskContext} for each task in the batch. Implementations must complete every context in the list.
     * @param dropHeadersContextSupplier Supplies a context (a resource for use in a try-with-resources block) which captures and drops any
     *                                   emitted response headers, for cases where things like deprecation warnings may be emitted but
     *                                   cannot be associated with any specific task.
     */
    record BatchExecutionContext<T extends ClusterStateTaskListener> (
        ClusterState initialState,
        List<TaskContext<T>> taskContexts,
        Supplier<Releasable> dropHeadersContextSupplier
    ) {
        /**
         * Creates a context (a resource for use in a try-with-resources block) which captures and drops any emitted response headers, for
         * cases where things like deprecation warnings may be emitted but cannot be associated with any specific task.
         */
        public Releasable dropHeadersContext() {
            return dropHeadersContextSupplier.get();
        }
    }
}
