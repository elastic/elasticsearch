/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

/**
 * An abstract base class for tasks that can update the cluster state.
 * Implementations define the actual state transformation logic in the {@link #execute} method
 * and can optionally be notified when the update completes via {@link #clusterStateProcessed}.
 *
 * <p>Tasks are executed on the master node with configurable priority and optional timeout.
 * If the timeout expires before execution, the task fails with a timeout exception.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * ClusterStateUpdateTask task = new ClusterStateUpdateTask(Priority.URGENT) {
 *     {@literal @}Override
 *     public ClusterState execute(ClusterState currentState) {
 *         // Return a new state or the same instance if no changes needed
 *         return ClusterState.builder(currentState)
 *             .metadata(updatedMetadata)
 *             .build();
 *     }
 *
 *     {@literal @}Override
 *     public void onFailure(Exception e) {
 *         logger.error("Cluster state update failed", e);
 *     }
 * };
 * clusterService.submitStateUpdateTask("source", task);
 * }</pre>
 *
 * @see ClusterStateTaskListener
 * @see Priority
 */
public abstract class ClusterStateUpdateTask implements ClusterStateTaskListener {

    private final Priority priority;

    @Nullable
    private final TimeValue timeout;

    /**
     * Constructs a cluster state update task with {@link Priority#NORMAL} priority and no timeout.
     */
    public ClusterStateUpdateTask() {
        this(Priority.NORMAL);
    }

    /**
     * Constructs a cluster state update task with the specified priority and no timeout.
     *
     * @param priority the execution priority for this task
     */
    public ClusterStateUpdateTask(Priority priority) {
        this(priority, null);
    }

    /**
     * Constructs a cluster state update task with {@link Priority#NORMAL} priority and the specified timeout.
     *
     * @param timeout the maximum time to wait for execution, or null for no timeout
     */
    public ClusterStateUpdateTask(TimeValue timeout) {
        this(Priority.NORMAL, timeout);
    }

    /**
     * Constructs a cluster state update task with the specified priority and timeout.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // High priority task with 30 second timeout
     * ClusterStateUpdateTask task = new ClusterStateUpdateTask(
     *     Priority.HIGH,
     *     TimeValue.timeValueSeconds(30)
     * ) {
     *     // ... implementation
     * };
     * }</pre>
     *
     * @param priority the execution priority for this task
     * @param timeout the maximum time to wait for execution, or null for no timeout
     */
    public ClusterStateUpdateTask(Priority priority, TimeValue timeout) {
        this.priority = priority;
        this.timeout = timeout;
    }

    /**
     * Computes and returns the new cluster state that results from executing this task.
     *
     * <p><b>Important Optimization:</b> Return the <b>same instance</b> if no changes are needed.
     * This short-circuits the entire publication process, saving significant time and effort.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public ClusterState execute(ClusterState currentState) throws Exception {
     *     // Check if update is actually needed
     *     if (currentState.metadata().hasIndex("my-index")) {
     *         return currentState; // Return same instance - no change needed
     *     }
     *
     *     // Build and return new state
     *     return ClusterState.builder(currentState)
     *         .metadata(Metadata.builder(currentState.metadata())
     *             .put(newIndexMetadata)
     *             .build())
     *         .build();
     * }
     * }</pre>
     *
     * @param currentState the current cluster state before this task executes
     * @return the new cluster state, or the same instance if no changes are needed
     * @throws Exception if the state update cannot be computed
     */
    public abstract ClusterState execute(ClusterState currentState) throws Exception;

    /**
     * Called after the result of {@link #execute} has been successfully processed by all listeners.
     * This callback indicates that the cluster state update has been fully applied.
     *
     * <p><b>Critical Requirements:</b></p>
     * <ul>
     *   <li>Implementations MUST NOT throw exceptions</li>
     *   <li>Exceptions are logged at ERROR level and otherwise ignored (except in tests)</li>
     *   <li>If log-and-ignore is not appropriate, handle exceptions explicitly</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
     *     logger.info("Cluster state updated from version {} to {}",
     *         initialState.version(), newState.version());
     *     // Notify completion handlers, update metrics, etc.
     * }
     * }</pre>
     *
     * @param initialState the cluster state before the update
     * @param newState the cluster state that was ultimately published
     */
    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {}

    /**
     * Returns the timeout for this task. If the task is not processed within this time,
     * {@link ClusterStateTaskListener#onFailure(Exception)} will be called.
     *
     * @return the timeout value, or {@code null} if no timeout is configured
     */
    @Nullable
    public final TimeValue timeout() {
        return timeout;
    }

    /**
     * Returns the execution priority for this task. Higher priority tasks are processed
     * before lower priority tasks.
     *
     * @return the priority level for this task
     */
    public final Priority priority() {
        return priority;
    }
}
